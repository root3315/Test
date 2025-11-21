import os
import logging
import time
import hashlib
import asyncio
import aiohttp
import aiosqlite
from typing import Optional
from aiogram import Bot, Dispatcher, executor, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

# ---------------- CONFIG ----------------
TELEGRAM_BOT_TOKEN = '7454681736:AAE6wnHDCcTXss5VFPwP0GzTDpcEQrcWdcg'
DB_PATH = 'waifu_bot.db'
WAIFU_API_URL = 'https://api.waifu.im/search'
DEFAULT_TAGS = ['waifu', 'neko', 'maid', 'smile', 'megane', 'uniform', 'school_uniform']
RATE_LIMIT_SECONDS = 1.0
MENU_EMOJI = {'random': '🎲', 'tags': '🏷️', 'settings': '⚙️', 'sfw': '✅', 'nsfw': '🔞'}
LOG_LEVEL = 'INFO'

# ---------------- DATABASE ----------------
CREATE_SCHEMA = '''
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    first_name TEXT,
    username TEXT,
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

async def register_user(user):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            'INSERT OR REPLACE INTO users(user_id, first_name, username, last_seen) VALUES (?, ?, ?, ?)',
            (user.id, user.first_name or '', user.username or '', int(time.time()))
        )
        await db.commit()

async def get_user_pref_sfw(user_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT pref_sfw FROM users WHERE user_id = ?', (user_id,))
        row = await cur.fetchone()
        return 1 if not row else row[0]

async def set_user_pref_sfw(user_id: int, pref: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('UPDATE users SET pref_sfw = ? WHERE user_id = ?', (pref, user_id))
        await db.commit()

async def record_event(user_id: int, event_type: str, detail: str = ''):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            'INSERT INTO events(user_id, event_type, detail, created_at) VALUES (?, ?, ?, ?)',
            (user_id, event_type, detail, int(time.time()))
        )
        await db.commit()

# ---------------- RATE LIMIT ----------------
USER_LAST_TIME = {}

def is_rate_limited(uid: int) -> bool:
    now = time.time()
    last = USER_LAST_TIME.get(uid, 0)
    if now - last < RATE_LIMIT_SECONDS:
        return True
    USER_LAST_TIME[uid] = now
    return False

# ---------------- MENUS ----------------
def get_main_menu():
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton(f"Random {MENU_EMOJI['random']}", callback_data='random'),
        InlineKeyboardButton(f"Tags {MENU_EMOJI['tags']}", callback_data='tags'),
        InlineKeyboardButton(f"Settings {MENU_EMOJI['settings']}", callback_data='settings')
    )
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

# ---------------- API ----------------
async def fetch_waifu(session: aiohttp.ClientSession, tag: Optional[str], nsfw: bool, limit: int = 1) -> list[dict]:
    params = {
        'limit': limit,
        'is_nsfw': 'true' if nsfw else 'false'
    }
    if tag:
        # ⚡ передаем тег как строку (API требует именно строку)
        params['included_tags'] = str(tag)
    async with session.get(WAIFU_API_URL, params=params, ssl=True, timeout=20) as resp:
        if resp.status != 200:
            text = await resp.text()
            raise RuntimeError(f"API error {resp.status}: {text}")
        data = await resp.json()
    return data.get('images', [])

# ---------------- SEND IMAGE ----------------
async def send_by_tag(message_or_query, tag, bot, user_id):
    pref = await get_user_pref_sfw(user_id)
    nsfw = False if pref == 1 else True
    if tag and tag.lower().startswith('nsfw:'):
        nsfw = True
        tag = tag.split(':',1)[1].strip()

    async with aiohttp.ClientSession() as session:
        try:
            images = await fetch_waifu(session, tag, nsfw, limit=1)
        except Exception as e:
            await message_or_query.reply(f"Ошибка API: {e}")
            return

        if not images:
            await message_or_query.reply('Не нашёл! 😢 Попробуй другой тег.')
            await record_event(user_id, 'not_found', tag or '')
            return

        im = images[0]
        url = im.get('url') or im.get('image_url') or im.get('source_url')
        if not url:
            await message_or_query.reply('Не удалось получить URL картинки.')
            return

        caption = f"Tag: {tag or 'random'} | {'🔞 NSFW' if nsfw else '✅ SFW'} 🎨"
        kb = InlineKeyboardMarkup()
        kb.add(
            InlineKeyboardButton('Next ▶', callback_data='next'),
            InlineKeyboardButton('Share 🔗', switch_inline_query=url)
        )

        chat_id = message_or_query.chat.id if hasattr(message_or_query, 'chat') else message_or_query.chat_id
        await bot.send_photo(chat_id, url, caption=caption, reply_markup=kb)
        await record_event(user_id, 'served_api', tag or 'random')

# ---------------- HANDLERS ----------------
async def cmd_start(message: types.Message):
    await register_user(message.from_user)
    await message.answer(
        f"Привет, {message.from_user.first_name}! 🌟 Я Waifu бот. Выбери опцию ниже! 🎉",
        reply_markup=get_main_menu()
    )

async def cmd_menu(message: types.Message):
    await message.answer('Главное меню:', reply_markup=get_main_menu())

async def handle_text(message: types.Message, bot):
    await register_user(message.from_user)
    if is_rate_limited(message.from_user.id):
        await message.answer('Подожди секунду! ⏳')
        return
    tag = message.text.strip()
    if tag == 'Random 🎲':
        tag = None
    await send_by_tag(message, tag, bot, message.from_user.id)

async def handle_callback(query: types.CallbackQuery, bot):
    data = query.data
    user_id = query.from_user.id

    if data == 'menu':
        await query.message.edit_text('Главное меню:', reply_markup=get_main_menu())
    elif data == 'random' or data == 'next':
        await send_by_tag(query.message, None, bot, user_id)
    elif data == 'tags':
        await query.message.edit_text('Выбери тег:', reply_markup=get_tags_menu())
    elif data.startswith('tag:'):
        tag = data.split(':',1)[1]
        await send_by_tag(query.message, tag, bot, user_id)
    elif data == 'settings':
        await query.message.edit_text('Настройки:', reply_markup=await get_settings_menu(user_id))
    elif data == 'toggle_sfw':
        pref = await get_user_pref_sfw(user_id)
        new_pref = 0 if pref == 1 else 1
        await set_user_pref_sfw(user_id, new_pref)
        await query.message.edit_text('Настройки обновлены!', reply_markup=await get_settings_menu(user_id))
    await query.answer()

# ---------------- BOT INIT ----------------
logging.basicConfig(level=LOG_LEVEL)
bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher(bot)

@dp.message_handler(commands=['start','help'])
async def _start(msg: types.Message):
    await cmd_start(msg)

@dp.message_handler(commands=['menu'])
async def _menu(msg: types.Message):
    await cmd_menu(msg)

@dp.message_handler()
async def _text(msg: types.Message):
    await handle_text(msg, bot)

@dp.callback_query_handler()
async def _callback(query: types.CallbackQuery):
    await handle_callback(query, bot)

async def on_startup(dp):
    await init_db()
    logging.info('DB initialized')

if __name__ == '__main__':
    executor.start_polling(dp, on_startup=on_startup)
