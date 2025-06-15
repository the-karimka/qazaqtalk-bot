import time
import threading
import sqlite3
from datetime import datetime, timedelta
import difflib
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

bot = telebot.TeleBot("7671940309:AAGr3PdGbv0o8DRVR8eZKu5cc07fzT2tCBw")
user_data = {}

def reset_database():
    conn = sqlite3.connect('chck.db')
    cur = conn.cursor()

    # Удаляем старые таблицы
    cur.execute("DROP TABLE IF EXISTS users")
    cur.execute("DROP TABLE IF EXISTS matches")
    cur.execute("DROP TABLE IF EXISTS review_queue")

    # Создаем их заново
    cur.execute('''CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name TEXT,
        age TEXT,
        kazakh_level TEXT,
        gender TEXT,
        preferred_gender TEXT,
        telegram_username TEXT
    )''')

    cur.execute('''CREATE TABLE matches (
        user_id INTEGER PRIMARY KEY,
        partner_id INTEGER,
        match_time TEXT
    )''')

    cur.execute('''CREATE TABLE review_queue (
        chat_id1 INTEGER,
        chat_id2 INTEGER,
        send_time TEXT
    )''')

    conn.commit()
    conn.close()
    print("База данных успешно сброшена и создана заново.")

# reset_database()

def update_schema():
    conn = sqlite3.connect('chck.db')
    cur = conn.cursor()

    # Получаем список существующих колонок
    cur.execute("PRAGMA table_info(users)")
    columns = [col[1] for col in cur.fetchall()]

    # Добавляем поля, если их нет
    if 'gender' not in columns:
        cur.execute("ALTER TABLE users ADD COLUMN gender TEXT")
    if 'preferred_gender' not in columns:
        cur.execute("ALTER TABLE users ADD COLUMN preferred_gender TEXT")

    conn.commit()
    cur.close()
    conn.close()

def init_review_queue():
    conn = sqlite3.connect('chck.db')
    cur = conn.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY,
        name TEXT,
        age TEXT,
        kazakh_level TEXT,
        gender TEXT,
        preferred_gender TEXT,
        telegram_username TEXT
    )''')
    cur.execute('''CREATE TABLE IF NOT EXISTS review_queue (
        chat_id1 INTEGER,
        chat_id2 INTEGER,
        send_time TEXT
    )''')
    cur.execute('''CREATE TABLE IF NOT EXISTS matches (
        user_id INTEGER PRIMARY KEY,
        partner_id INTEGER,
        match_time TEXT
    )''')
    conn.commit()
    cur.close()
    conn.close()

def fuzzy_match(str1, str2, threshold=0.7):
    return difflib.SequenceMatcher(None, str1.strip().lower(), str2.strip().lower()).ratio() >= threshold

@bot.message_handler(commands=['start'])
def start(message):
    update_schema()
    init_review_queue()
    username = message.from_user.username if message.from_user.username else None
    if username:
        user_data[message.chat.id] = {'telegram_username': username}
        bot.send_message(message.chat.id, "Сәлем!👋\n"
                                          "Добро пожаловать в QazaqTalk – бота для изучения казахского языка!\n"
                                          "Здесь тебя ждут увлекательные уроки, интерактивные задания и полезные упражнения, которые помогут легко и с удовольствием освоить казахский язык.\n"
                                          "Готов начать? Ендеше, бірге үйренейік! 🚀\n"
                                          "Для начала давай пройдем регистрацию! Введи свое имя, чтобы я мог обращаться к тебе лично. 😊")
        bot.register_next_step_handler(message, get_name)
    else:
        bot.send_message(message.chat.id, "Введите ваш Telegram username:")
        bot.register_next_step_handler(message, get_username)

@bot.message_handler(commands=['restart'])
def restart(message):
    chat_id = message.chat.id

    # Удаляем данные пользователя из базы
    conn = sqlite3.connect('chck.db')
    cur = conn.cursor()
    cur.execute("DELETE FROM users WHERE id = ?", (chat_id,))
    cur.execute("DELETE FROM matches WHERE user_id = ? OR partner_id = ?", (chat_id, chat_id))
    conn.commit()
    cur.close()
    conn.close()

    # Очищаем кэш пользователя
    if chat_id in user_data:
        del user_data[chat_id]

    # Начинаем регистрацию заново
    username = message.from_user.username if message.from_user.username else None
    if username:
        user_data[chat_id] = {'telegram_username': username}
        bot.send_message(chat_id, "🔁 Анкета сброшена. Начнем заново!\n\nВведи свое имя:")
        bot.register_next_step_handler(message, get_name)
    else:
        bot.send_message(chat_id, "🔁 Анкета сброшена. Пожалуйста, введите ваш Telegram username:")
        bot.register_next_step_handler(message, get_username)

@bot.message_handler(commands=['guidebook'])
def send_guidebook(message):
    try:
        with open('guidebook.docx', 'rb') as f:
            bot.send_document(message.chat.id, f, caption="📘 Вот ваш гайдбук по QazaqTalk!")
    except Exception as e:
        bot.send_message(message.chat.id, "⚠️ Не удалось отправить гайдбук.")
        print(f"[ERROR] Ошибка при отправке гайдбука: {e}")



def get_username(message):
    user_data[message.chat.id] = {'telegram_username': message.text.strip()}
    bot.send_message(message.chat.id, "Введите ваше имя:")
    bot.register_next_step_handler(message, get_name)

def get_name(message):
    user_data[message.chat.id]['name'] = message.text.strip()
    ask_question(message.chat.id, "Сколько вам лет?", ["10-13", "14-16", "17-20", "21-25", "30-35", "35+"])


def ask_question(chat_id, question, options):
    markup = InlineKeyboardMarkup()
    for option in options:
        markup.add(InlineKeyboardButton(text=option, callback_data=option))
    bot.send_message(chat_id, question, reply_markup=markup)

@bot.callback_query_handler(func=lambda call: True)
def handle_callback(call):
    chat_id = call.message.chat.id
    data = call.data
    bot.answer_callback_query(call.id)

    if 'age' not in user_data[chat_id]:
        user_data[chat_id]['age'] = data
        ask_question(chat_id, "Какой у вас уровень казахского?", ["Начинающий", "Средний", "Продвинутый", "Носитель"])
    elif 'kazakh_level' not in user_data[chat_id]:
        user_data[chat_id]['kazakh_level'] = data
        ask_question(chat_id, "Ваш пол?", ["Мужской", "Женский"])
    elif 'gender' not in user_data[chat_id]:
        user_data[chat_id]['gender'] = data
        ask_question(chat_id, "С кем вы бы хотели практиковаться?", ["Мужской", "Женский", "Не важно"])
    elif 'preferred_gender' not in user_data[chat_id]:
        user_data[chat_id]['preferred_gender'] = data
        save_to_db(chat_id)

def save_to_db(chat_id):
    conn = sqlite3.connect('chck.db')
    cur = conn.cursor()
    cur.execute("""REPLACE INTO users (id, name, age, kazakh_level, gender, preferred_gender, telegram_username) 
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (chat_id,
                 user_data[chat_id]['name'],
                 user_data[chat_id]['age'],
                 user_data[chat_id]['kazakh_level'],
                 user_data[chat_id]['gender'],
                 user_data[chat_id]['preferred_gender'],
                 user_data[chat_id]['telegram_username']))
    conn.commit()
    cur.close()
    conn.close()
    find_match(chat_id)

def send_review_request(chat_id, other_id):
    review_message = (
        "Оцените собеседника по 5-балльной шкале:\n\n"
        "1. Насколько легко было общаться с собеседником?\n"
        "(1 — очень трудно, 5 — очень легко)\n\n"
        "2. Насколько активным был собеседник во время встречи?\n"
        "(1 — почти не участвовал, 5 — очень активный)\n\n"
        "3. Насколько дружелюбным был собеседник?\n"
        "(1 — холодный, 5 — очень доброжелательный)\n\n"
        "Если есть какие-то жалобы, напишите их ниже 😊\n\n"
        "Если не было встречи, просто напишите что-нибудь."
    )
    try:
        bot.send_message(chat_id, review_message)
        bot.send_message(other_id, review_message)
    except Exception as e:
        print(f"[ERROR] Отправка отзыва: {e}")

def schedule_review(chat_id1, chat_id2):
    review_time = (datetime.utcnow() + timedelta(hours=48)).isoformat()
    conn = sqlite3.connect('chck.db')
    cur = conn.cursor()
    cur.execute("INSERT INTO review_queue (chat_id1, chat_id2, send_time) VALUES (?, ?, ?)", (chat_id1, chat_id2, review_time))
    conn.commit()
    cur.close()
    conn.close()

def schedule_review_check():
    def checker():
        while True:
            now = datetime.utcnow()
            conn = sqlite3.connect('chck.db')
            cur = conn.cursor()
            cur.execute("SELECT chat_id1, chat_id2, send_time FROM review_queue")
            rows = cur.fetchall()
            for chat_id1, chat_id2, send_time_str in rows:
                try:
                    send_time = datetime.fromisoformat(send_time_str)
                except ValueError:
                    print(f"[ERROR] Неверный формат времени: {send_time_str}")
                    continue

                if now >= send_time:
                    send_review_request(chat_id1, chat_id2)
                    cur.execute("DELETE FROM review_queue WHERE chat_id1 = ? AND chat_id2 = ?", (chat_id1, chat_id2))
            conn.commit()
            cur.close()
            conn.close()
            time.sleep(60)  # Проверяем раз в минуту
    threading.Thread(target=checker, daemon=True).start()


def find_match(chat_id):
    conn = sqlite3.connect('chck.db')
    cur = conn.cursor()

    # ✅ Проверка, не находится ли пользователь уже в активной паре
    cur.execute("SELECT partner_id, match_time FROM matches WHERE user_id = ?", (chat_id,))
    result = cur.fetchone()
    if result:
        match_time = datetime.fromisoformat(result[1])
        if datetime.utcnow() < match_time + timedelta(hours=48):
            bot.send_message(chat_id, "⏳ Вы уже в паре. Подождите 48 часов до следующего собеседника.")
            cur.close()
            conn.close()
            return
        else:
            # Удаляем устаревшую пару
            cur.execute("DELETE FROM matches WHERE user_id = ?", (chat_id,))
            conn.commit()

    # ✅ Получаем список пользователей, которые уже в паре (актуальной)
    cur.execute("SELECT user_id, match_time FROM matches")
    busy_users = {row[0]: row[1] for row in cur.fetchall()}

    now = datetime.utcnow()
    to_remove = [uid for uid, m_time in busy_users.items()
                 if datetime.fromisoformat(m_time) + timedelta(hours=48) <= now]
    for uid in to_remove:
        cur.execute("DELETE FROM matches WHERE user_id = ?", (uid,))
        busy_users.pop(uid)

    # ✅ Получаем всех доступных пользователей, исключая текущего и тех, кто уже в паре
    placeholders = ','.join(['?'] * len(busy_users)) if busy_users else '0'
    query = f"""
        SELECT id, name, age, kazakh_level, gender, preferred_gender, telegram_username
        FROM users
        WHERE id != ? AND id NOT IN ({placeholders})
    """
    args = [chat_id] + list(busy_users.keys()) if busy_users else [chat_id]
    cur.execute(query, args)
    users = cur.fetchall()

    current = user_data[chat_id]
    required_score = 2

    for user in users:
        other_id, name, age, level, gender, preferred_gender, username = user

        if current['preferred_gender'] != "Не важно" and gender != current['preferred_gender']:
            continue
        if preferred_gender != "Не важно" and current['gender'] != preferred_gender:
            continue

        match_score = 0
        if fuzzy_match(current['kazakh_level'], level):
            match_score += 1
        if fuzzy_match(current['age'], age):
            match_score += 1

        if match_score >= required_score:
            # ✅ Совпадение найдено
            bot.send_message(chat_id, f"🎉 Вы совпали с @{username}!\n👤 Имя: {name}\n📅 Возраст: {age}\n⚧ Пол: {gender}\n🗣 Уровень казахского: {level}")
            bot.send_message(other_id, f"🎉 Вы совпали с @{current['telegram_username']}!\n👤 Имя: {current['name']}\n📅 Возраст: {current['age']}\n⚧ Пол: {current['gender']}\n🗣 Уровень казахского: {current['kazakh_level']}")

            match_time = datetime.utcnow().isoformat()
            cur.execute("REPLACE INTO matches (user_id, partner_id, match_time) VALUES (?, ?, ?)", (chat_id, other_id, match_time))
            cur.execute("REPLACE INTO matches (user_id, partner_id, match_time) VALUES (?, ?, ?)", (other_id, chat_id, match_time))
            conn.commit()

            schedule_review(chat_id, other_id)
            cur.close()
            conn.close()
            return

    # Если совпадений нет
    bot.send_message(chat_id, "😕 Пока нет совпадений. Попробуйте позже.")
    cur.close()
    conn.close()

# Запускаем проверку отзывов
schedule_review_check()

from telebot.types import BotCommand

bot.set_my_commands([
    BotCommand("start", "🚀 Запустить бота"),
    BotCommand("restart", "🔁 Сбросить анкету"),
    BotCommand("guidebook", "📘 Получить гайдбук")
])


# Запускаем бота
bot.polling(none_stop=True)
