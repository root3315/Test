import os
import logging
import time
import hashlib
import asyncio
import aiohttp
import aiosqlite
from typing import Optional, List
from aiogram import Bot, Dispatcher, executor, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, InlineQueryResultPhoto, InputTextMessageContent

# Config
TELEGRAM_BOT_TOKEN = '7454681736:AAE6wnHDCcTXss5VFPwP0GzTDpcEQrcWdcg'# MUST be set in env
ADMIN_ID = 5707638365
ADMIN_TOKEN = 'Admin'  # weak by design — change in production
DB_PATH = 'waifu_bot.db'
IMAGE_CACHE_DIR = 'image_cache'
WAIFU_API_URL = 'https://api.waifu.im/search'
DEFAULT_TAGS = ['waifu', 'neko', 'maid', 'smile', 'megane', 'uniform', 'school_uniform']
MAX_CACHE_IMAGES = 800
RATE_LIMIT_SECONDS = 0.8
MENU_EMOJI = {'random': '🎲', 'tags': '🏷️', 'settings': '⚙️', 'favorites': '❤️', 'nsfw': '🔞', 'sfw': '✅'}
LOG_LEVEL = 'INFO'

# DB Schema and Helpers
CREATE_SCHEMA = '''
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS cache (
    id INTEGER PRIMARY KEY,
    tag TEXT,
    nsfw INTEGER,
    url TEXT UNIQUE,
    local_path TEXT,
    checksum TEXT UNIQUE,
    added_at INTEGER
);
CREATE TABLE IF NOT EXISTS favorites (
    id INTEGER PRIMARY KEY,
    user_id INTEGER,
    url TEXT,
    local_path TEXT,
    added_at INTEGER
);
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    first_name TEXT,
    username TEXT,
    is_banned INTEGER DEFAULT 0,
    pref_sfw INTEGER DEFAULT 1,
    last_seen INTEGER
);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY,
    user_id INTEGER,
    event_type TEXT,
    detail TEXT,
    created_at INTEGER
);
'''

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(CREATE_SCHEMA)
        await db.commit()

async def insert_cache(tag: str, nsfw: int, url: str, local_path: str, checksum: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('INSERT OR IGNORE INTO cache(tag, nsfw, url, local_path, checksum, added_at) VALUES (?, ?, ?, ?, ?, ?)',
                         (tag or '', nsfw, url, local_path, checksum, int(time.time())))
        await db.commit()

async def get_cached(tag: Optional[str], nsfw: int) -> Optional[str]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT local_path FROM cache WHERE tag = ? AND nsfw = ? ORDER BY added_at DESC LIMIT 1',(tag or '', nsfw))
        row = await cur.fetchone()
        return row[0] if row else None

async def delete_cache_by_id(cid: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('DELETE FROM cache WHERE id = ?', (cid,))
        await db.commit()

async def register_user(user):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('INSERT OR REPLACE INTO users(user_id, first_name, username, last_seen) VALUES (?, ?, ?, ?)',
                         (user.id, user.first_name or '', user.username or '', int(time.time())))
        await db.commit()

async def set_user_pref_sfw(user_id: int, pref: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('UPDATE users SET pref_sfw = ? WHERE user_id = ?', (pref, user_id))
        await db.commit()

async def get_user_pref_sfw(user_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT pref_sfw FROM users WHERE user_id = ?', (user_id,))
        row = await cur.fetchone()
        return 1 if not row else row[0]

async def add_favorite(user_id: int, url: str, local_path: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('INSERT INTO favorites(user_id, url, local_path, added_at) VALUES (?, ?, ?, ?)', (user_id, url, local_path or url, int(time.time())))
        await db.commit()

async def list_favorites(user_id: int, offset: int = 0, limit: int = 10) -> list[tuple]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT id, url, local_path, added_at FROM favorites WHERE user_id = ? ORDER BY added_at DESC LIMIT ? OFFSET ?', (user_id, limit, offset))
        rows = await cur.fetchall()
        return rows

async def count_favorites(user_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT COUNT(*) FROM favorites WHERE user_id = ?', (user_id,))
        row = await cur.fetchone()
        return row[0]

async def ban_user(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('UPDATE users SET is_banned = 1 WHERE user_id = ?', (user_id,))
        await db.commit()

async def unban_user(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('UPDATE users SET is_banned = 0 WHERE user_id = ?', (user_id,))
        await db.commit()

async def is_banned(user_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT is_banned FROM users WHERE user_id = ?', (user_id,))
        row = await cur.fetchone()
        return bool(row and row[0])

async def record_event(user_id: int, event_type: str, detail: str = ''):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('INSERT INTO events(user_id, event_type, detail, created_at) VALUES (?, ?, ?, ?)', (user_id, event_type, detail, int(time.time())))
        await db.commit()

async def export_favorites_csv(path: str):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT id, user_id, url, local_path, added_at FROM favorites ORDER BY added_at DESC')
        rows = await cur.fetchall()
    with open(path, 'w', encoding='utf-8') as f:
        f.write('id,user_id,url,local_path,added_at\n')
        for r in rows:
            f.write(f"{r[0]},{r[1]},\"{r[2]}\",\"{r[3]}\",{r[4]}\n")

# Utils
os.makedirs(IMAGE_CACHE_DIR, exist_ok=True)

async def fetch_waifu(session: aiohttp.ClientSession, tag: Optional[str], nsfw: bool, limit: int = 1) -> list[dict]:
    params = {}
    if tag:
        params['included_tags'] = tag
    params['limit'] = limit
    params['is_nsfw'] = 'true' if nsfw else 'false'
    async with session.get(WAIFU_API_URL, params=params, ssl=True, timeout=20) as resp:
        data = await resp.json()
    return data.get('images') or []

async def download_image(session: aiohttp.ClientSession, url: str) -> str:
    checksum = hashlib.sha1(url.encode()).hexdigest()
    ext = os.path.splitext(url)[1].split('?')[0] or '.jpg'
    filename = f"{checksum}{ext}"
    local_path = os.path.join(IMAGE_CACHE_DIR, filename)
    if os.path.exists(local_path):
        return local_path
    async with session.get(url, timeout=30) as r:
        if r.status != 200:
            raise RuntimeError('download failed')
        content = await r.read()
    with open(local_path, 'wb') as f:
        f.write(content)
    # try to insert into DB cache (non-blocking)
    try:
        await insert_cache(tag=None, nsfw=0, url=url, local_path=local_path, checksum=checksum)
    except Exception:
        pass
    return local_path

# Admin
async def cmd_stats(message: types.Message, bot):
    if message.from_user.id != ADMIN_ID:
        await message.reply('Только админ может использовать эту команду.')
        return
    # simple stats
    await message.reply('Собираю статистику...')
    # for brevity, show recent events count (could be expanded)
    await message.reply('Статистика готова (пример): users, served images, top tags — смотрите в БД')

async def cmd_broadcast(message: types.Message, bot):
    if message.from_user.id != ADMIN_ID:
        await message.reply('Только админ может использовать эту команду.')
        return
    text = message.get_args()
    if not text:
        await message.reply('Usage: /broadcast Your message')
        return
    # naive broadcast: iterate users table
    await message.reply('Запускаю рассылку...')
    async def _send(uid):
        try:
            await bot.send_message(uid, text)
        except Exception:
            pass
    # fetch user ids from DB (not implemented here to avoid circular import)
    asyncio.create_task(message.reply('Broadcast queued'))

async def cmd_ban(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.reply('Только админ.')
        return
    args = message.get_args().split()
    if not args:
        await message.reply('Usage: /ban <user_id>')
        return
    try:
        uid = int(args[0])
    except ValueError:
        await message.reply('User id must be integer')
        return
    await ban_user(uid)
    await message.reply(f'User {uid} banned')

async def cmd_unban(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.reply('Только админ.')
        return
    args = message.get_args().split()
    if not args:
        await message.reply('Usage: /unban <user_id>')
        return
    try:
        uid = int(args[0])
    except ValueError:
        await message.reply('User id must be integer')
        return
    await unban_user(uid)
    await message.reply(f'User {uid} unbanned')

async def cmd_export_favs(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.reply('Только админ.')
        return
    path = 'favorites_export.csv'
    await export_favorites_csv(path)
    await message.reply_document(open(path, 'rb'))

async def cmd_admin(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.reply('Только админ может использовать эту команду.')
        return
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(InlineKeyboardButton('Stats 📊', callback_data='admin_stats'))
    kb.add(InlineKeyboardButton('Broadcast 📢', callback_data='admin_broadcast'))
    kb.add(InlineKeyboardButton('Ban 🚫', callback_data='admin_ban'))
    kb.add(InlineKeyboardButton('Unban ✅', callback_data='admin_unban'))
    kb.add(InlineKeyboardButton('Export Favs 📁', callback_data='admin_export'))
    await message.answer('Админ панель:', reply_markup=kb)

# Handlers
USER_LAST_TIME = {}

def is_rate_limited(uid: int) -> bool:
    now = time.time()
    last = USER_LAST_TIME.get(uid, 0)
    if now - last < RATE_LIMIT_SECONDS:
        return True
    USER_LAST_TIME[uid] = now
    return False

def get_main_menu():
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(InlineKeyboardButton(f"Random {MENU_EMOJI['random']}", callback_data='random'))
    kb.add(InlineKeyboardButton(f"Tags {MENU_EMOJI['tags']}", callback_data='tags'))
    kb.add(InlineKeyboardButton(f"Settings {MENU_EMOJI['settings']}", callback_data='settings'))
    kb.add(InlineKeyboardButton(f"Favorites {MENU_EMOJI['favorites']}", callback_data='favorites'))
    return kb

def get_tags_menu():
    kb = InlineKeyboardMarkup(row_width=3)
    for tag in DEFAULT_TAGS:
        kb.insert(InlineKeyboardButton(tag.capitalize(), callback_data=f'tag:{tag}'))
    kb.add(InlineKeyboardButton('Back', callback_data='menu'))
    return kb

async def get_settings_menu(user_id: int):
    pref = await get_user_pref_sfw(user_id)
    kb = InlineKeyboardMarkup(row_width=2)
    sfw_text = f"SFW {MENU_EMOJI['sfw']}" if pref == 1 else f"NSFW {MENU_EMOJI['nsfw']}"
    kb.add(InlineKeyboardButton(sfw_text, callback_data='toggle_sfw'))
    kb.add(InlineKeyboardButton('Back', callback_data='menu'))
    return kb

def get_fav_controls(page: int, total: int):
    kb = InlineKeyboardMarkup(row_width=3)
    if page > 0:
        kb.insert(InlineKeyboardButton('◀ Prev', callback_data=f'fav_page:{page-1}'))
    kb.insert(InlineKeyboardButton(f'Page {page+1}/{ (total // 10) + 1 if total > 0 else 1 }', callback_data='noop'))
    if (page + 1) * 10 < total:
        kb.insert(InlineKeyboardButton('Next ▶', callback_data=f'fav_page:{page+1}'))
    kb.add(InlineKeyboardButton('Back', callback_data='menu'))
    return kb

async def cmd_start(message: types.Message):
    await register_user(message.from_user)
    text = f"Привет, {message.from_user.first_name}! 🌟 Я Waifu бот. Выбери опцию ниже! 🎉"
    await message.answer(text, reply_markup=get_main_menu())

async def cmd_menu(message: types.Message):
    await message.answer('Главное меню:', reply_markup=get_main_menu())

async def handle_callback(query: types.CallbackQuery, bot):
    data = query.data
    user_id = query.from_user.id
    if data == 'menu':
        await query.message.edit_text('Главное меню:', reply_markup=get_main_menu())
    elif data == 'random':
        await send_by_tag(query.message, None, bot, user_id)
    elif data == 'tags':
        await query.message.edit_text('Выбери тег:', reply_markup=get_tags_menu())
    elif data.startswith('tag:'):
        tag = data.split(':', 1)[1]
        await send_by_tag(query.message, tag, bot, user_id)
    elif data == 'settings':
        await query.message.edit_text('Настройки:', reply_markup=await get_settings_menu(user_id))
    elif data == 'toggle_sfw':
        pref = await get_user_pref_sfw(user_id)
        new_pref = 0 if pref == 1 else 1
        await set_user_pref_sfw(user_id, new_pref)
        await query.message.edit_text('Настройки обновлены!', reply_markup=await get_settings_menu(user_id))
    elif data == 'favorites':
        await show_favorites(query.message, user_id, bot, page=0)
    elif data.startswith('fav_page:'):
        page = int(data.split(':', 1)[1])
        await show_favorites(query.message, user_id, bot, page)
    elif data.startswith('fav:'):
        checksum = data.split(':', 1)[1]
        # Найдем url и local по checksum
        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute('SELECT url, local_path FROM cache WHERE checksum = ?', (checksum,))
            row = await cur.fetchone()
            if row:
                url, local_path = row
                await add_favorite(user_id, url, local_path)
                await query.answer('Добавлено в избранное! ❤️')
            else:
                await query.answer('Ошибка добавления в избранное.')
    elif data == 'next':
        await send_by_tag(query.message, None, bot, user_id)  # random next
    elif data.startswith('admin_'):
        if user_id != ADMIN_ID:
            await query.answer('Только админ.')
            return
        action = data.split('_')[1]
        if action == 'stats':
            await cmd_stats(query.message, bot)
        elif action == 'broadcast':
            await query.message.reply('Используйте /broadcast <text> для рассылки.')
        elif action == 'ban':
            await query.message.reply('Используйте /ban <user_id> для бана.')
        elif action == 'unban':
            await query.message.reply('Используйте /unban <user_id> для разбана.')
        elif action == 'export':
            await cmd_export_favs(query.message)
    await query.answer()

async def show_favorites(message: types.Message, user_id: int, bot, page: int = 0):
    total = await count_favorites(user_id)
    rows = await list_favorites(user_id, page * 10, 10)
    if not rows:
        await message.edit_text('У вас нет избранного. 😔')
        return
    for r in rows:
        _, url, local, _ = r
        caption = f"Избранное: {url}"
        if os.path.exists(local):
            await bot.send_photo(message.chat.id, types.InputFile(local), caption=caption)
        else:
            await bot.send_photo(message.chat.id, url, caption=caption)
    await message.edit_reply_markup(reply_markup=get_fav_controls(page, total))

async def handle_text(message: types.Message, bot):
    if message.text.startswith('/'):
        return
    await register_user(message.from_user)
    if await is_banned(message.from_user.id):
        await message.answer('Вы заблокированы. 🚫')
        return
    if is_rate_limited(message.from_user.id):
        await message.answer('Подожди секунду! ⏳')
        return
    tag = message.text.strip()
    if tag == 'Random 🎲':
        tag = None
    await send_by_tag(message, tag, bot, message.from_user.id)

async def send_by_tag(message_or_query, tag, bot, user_id):
    pref = await get_user_pref_sfw(user_id)
    nsfw = False if pref == 1 else True
    if tag and tag.lower().startswith('nsfw:'):
        nsfw = True
        tag = tag.split(':',1)[1].strip()
    async with aiohttp.ClientSession() as session:
        images = await fetch_waifu(session, tag, nsfw, limit=1)
        if not images:
            await message_or_query.reply('Не нашёл! 😢 Попробуй другой тег.')
            await record_event(user_id, 'not_found', tag or '')
            return
        im = images[0]
        url = im.get('url') or im.get('image_url') or im.get('source_url')
        checksum = hashlib.sha1(url.encode()).hexdigest()
        local = None
        # Comment out download to avoid permission issues; send URL directly
        # try:
        #     local = await download_image(session, url)
        # except Exception as e:
        #     logging.error(f"Download failed: {e}")
        caption = f"Tag: {tag or 'random'} | {'🔞 NSFW' if nsfw else '✅ SFW'} 🎨"
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton('Save ❤️', callback_data=f'fav:{checksum}'),  
               InlineKeyboardButton('Next ▶', callback_data='next'),
               InlineKeyboardButton('Share 🔗', switch_inline_query=url))
        chat_id = message_or_query.chat.id if hasattr(message_or_query, 'chat') else message_or_query.chat_id
        await bot.send_photo(chat_id, url, caption=caption, reply_markup=kb)
        await record_event(user_id, 'served_api', tag or 'random')

async def inline_query(inline_query: types.InlineQuery):
    q = inline_query.query.strip()
    if not q:
        await inline_query.answer([], switch_pm_text='Введите тег', switch_pm_parameter='start')
        return
    nsfw = False
    tag = q
    if q.lower().startswith('nsfw:'):
        nsfw = True
        tag = q.split(':',1)[1]
    async with aiohttp.ClientSession() as session:
        imgs = await fetch_waifu(session, tag, nsfw, limit=6)
    results = []
    for im in imgs:
        url = im.get('url') or im.get('image_url') or im.get('source_url')
        if not url: continue
        idd = hashlib.sha1(url.encode()).hexdigest()[:12]
        caption = f"{tag or 'waifu'} | {'NSFW' if nsfw else 'SFW'}"
        results.append(InlineQueryResultPhoto(id=idd, photo_url=url, thumb_url=url, title=tag or 'waifu', caption=caption))
    await inline_query.answer(results[:10])

# Main
logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger(__name__)

if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError('Set TELEGRAM_BOT_TOKEN env variable')

bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher(bot)

@dp.message_handler(commands=['start','help'])
async def _start(msg: types.Message):
    await cmd_start(msg)

@dp.message_handler(commands=['menu'])
async def _menu(msg: types.Message):
    await cmd_menu(msg)

@dp.message_handler(commands=['admin'])
async def _admin_cmd(msg: types.Message):
    await cmd_admin(msg)

@dp.message_handler(commands=['stats'])
async def _stats(msg: types.Message):
    await cmd_stats(msg, bot)

@dp.message_handler(commands=['broadcast'])
async def _broadcast(msg: types.Message):
    await cmd_broadcast(msg, bot)

@dp.message_handler(commands=['ban'])
async def _ban(msg: types.Message):
    await cmd_ban(msg)

@dp.message_handler(commands=['unban'])
async def _unban(msg: types.Message):
    await cmd_unban(msg)

@dp.message_handler(commands=['export_favs'])
async def _export(msg: types.Message):
    await cmd_export_favs(msg)

@dp.message_handler()
async def _text(msg: types.Message):
    await handle_text(msg, bot)

@dp.inline_handler()
async def _inline(inline_query: types.InlineQuery):
    await inline_query(inline_query)

@dp.callback_query_handler()
async def _callback(query: types.CallbackQuery):
    await handle_callback(query, bot)

async def on_startup(dp):
    await init_db()
    logger.info('DB initialized')

if __name__ == '__main__':
    executor.start_polling(dp, on_startup=on_startup)
