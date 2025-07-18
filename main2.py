import time
import threading
import sqlite3
from datetime import datetime, timedelta
import difflib
import telebot
from datetime import datetime, UTC
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

bot = telebot.TeleBot("7671940309:AAGr3PdGbv0o8DRVR8eZKu5cc07fzT2tCBw")
user_data = {}
feedback_data = {}
user_state = {}

@bot.message_handler(func=lambda message: message.chat.id in user_state and user_state[message.chat.id]['step'] == 'awaiting_feedback')
def process_feedback(message):
    user_id = message.chat.id
    partner_id = user_state[user_id]['partner_id']
    text = message.text.strip()

    conn = sqlite3.connect('chck.db', check_same_thread=False, timeout=10)
    cur = conn.cursor()

    # Проверка: уже есть отзыв?
    cur.execute("""
        SELECT 1 FROM feedback
        WHERE from_user = ? AND to_user = ?
    """, (user_id, partner_id))
    if cur.fetchone():
        bot.send_message(user_id, "⚠️ Вы уже оставили отзыв на этого пользователя.")
        print(f"[MATCHING] Возвращаем без совпадений/ошибка для {chat_id}")
        cur.close()
        conn.close()     
        return

    try:
        parts = text.split(None, 1)
        scores = list(map(int, parts[0].split(',')))
        if len(scores) != 3:
            raise ValueError("Ожидается 3 оценки")
        ease, activity, friendliness = scores
        comment = parts[1] if len(parts) > 1 else ""

        # Обязательный комментарий при оценке 1 или 2
        if any(score in [1, 2] for score in scores) and not comment.strip():
            bot.send_message(user_id, "❗ При оценке 1 или 2 требуется комментарий. Пожалуйста, добавьте комментарий к отзыву.")
            print(f"[MATCHING] Возвращаем без совпадений/ошибка для {chat_id}")
            cur.close()
            conn.close()
            return

    except Exception as e:
        bot.send_message(user_id, "⚠️ Неверный формат. Введите как `5,4,5 Комментарий`.")
        print(f"[MATCHING] Возвращаем без совпадений/ошибка для {chat_id}")
        cur.close()
        conn.close()
        return

    # Сохраняем отзыв
    cur.execute("""
        INSERT INTO feedback 
        (from_user, to_user, question1, question2, question3, comment, timestamp)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        user_id,
        partner_id,
        ease,
        activity,
        friendliness,
        comment,
        datetime.now(UTC).isoformat()
    ))

    # Если оценки плохие (любая <= 3) — блокируем повторный мэтч
    if any(score <= 3 for score in [ease, activity, friendliness]):
        cur.execute("INSERT INTO past_matches (user1, user2, match_time) VALUES (?, ?, ?)", (user_id, partner_id, datetime.now(UTC).isoformat()))
        cur.execute("INSERT INTO past_matches (user1, user2, match_time) VALUES (?, ?, ?)", (partner_id, user_id, datetime.now(UTC).isoformat()))

    conn.commit()
    cur.close()
    conn.close()

    bot.send_message(user_id, "✅ Спасибо! Ваш отзыв сохранён.")
    del user_state[user_id]


# def reset_database():
#     conn = sqlite3.connect('chck.db', check_same_thread=False)
#     cur = conn.cursor()

#     cur.execute("DROP TABLE IF EXISTS feedback")

#     # Удаляем старые таблицы
#     cur.execute("DROP TABLE IF EXISTS users")
#     cur.execute("DROP TABLE IF EXISTS matches")
#     cur.execute("DROP TABLE IF EXISTS review_queue")

#     # Создаем их заново
#     cur.execute('''CREATE TABLE users (
#         id INTEGER PRIMARY KEY,
#         name TEXT,
#         age TEXT,
#         kazakh_level TEXT,
#         gender TEXT,
#         preferred_gender TEXT,
#         telegram_username TEXT
#     )''')

#     cur.execute('''CREATE TABLE matches (
#         user_id INTEGER PRIMARY KEY,
#         partner_id INTEGER,
#         match_time TEXT
#     )''')

#     cur.execute('''CREATE TABLE review_queue (
#         chat_id1 INTEGER,
#         chat_id2 INTEGER,
#         send_time TEXT
#     )''')

#     cur.execute('''
#     CREATE TABLE IF NOT EXISTS feedback (
#         from_user INTEGER,
#         to_user INTEGER,
#         question1 INTEGER,
#         question2 INTEGER,
#         question3 INTEGER,
#         comment TEXT,
#         timestamp TEXT
#     )''')

#     cur.execute('''
#     CREATE TABLE IF NOT EXISTS past_matches (
#         user1 INTEGER,
#         user2 INTEGER,
#         match_time TEXT
#     )
#     ''')



#     conn.commit()
#     conn.close()
#     print("База данных успешно сброшена и создана заново.")

# reset_database()

def create_tables_if_not_exist():
    conn = sqlite3.connect('chck.db', check_same_thread=False, timeout=10)
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

    cur.execute('''CREATE TABLE IF NOT EXISTS matches (
        user_id INTEGER PRIMARY KEY,
        partner_id INTEGER,
        match_time TEXT
    )''')

    cur.execute('''CREATE TABLE IF NOT EXISTS review_queue (
        chat_id1 INTEGER,
        chat_id2 INTEGER,
        send_time TEXT
    )''')

    cur.execute('''CREATE TABLE IF NOT EXISTS feedback (
        from_user INTEGER,
        to_user INTEGER,
        question1 INTEGER,
        question2 INTEGER,
        question3 INTEGER,
        comment TEXT,
        timestamp TEXT
    )''')

    cur.execute('''CREATE TABLE IF NOT EXISTS past_matches (
        user1 INTEGER,
        user2 INTEGER,
        match_time TEXT
    )''')

    conn.commit()
    conn.close()


def update_schema():
    conn = sqlite3.connect('chck.db', check_same_thread=False, timeout=10)
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
    conn = sqlite3.connect('chck.db', check_same_thread=False, timeout=10)
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
    cur.execute('''
    CREATE TABLE IF NOT EXISTS feedback (
        from_user INTEGER,
        to_user INTEGER,
        question1 INTEGER,
        question2 INTEGER,
        question3 INTEGER,
        comment TEXT,
        timestamp TEXT
    )''')
    conn.commit()
    cur.close()
    conn.close()

def age_range_to_tuple(age_str):
    if '+' in age_str:
        print(f"[MATCHING] Возвращаем без совпадений/ошибка для {chat_id}")
        return (int(age_str.replace('+', '')), 99)
    start, end = map(int, age_str.split('-'))
    print(f"[MATCHING] Возвращаем без совпадений/ошибка для {chat_id}")
    return (start, end)

def age_overlap(age1, age2):
    r1 = age_range_to_tuple(age1)
    r2 = age_range_to_tuple(age2)
    print(f"[MATCHING] Возвращаем без совпадений/ошибка для {chat_id}")
    return max(r1[0], r2[0]) <= min(r1[1], r2[1])

def level_match(level1, level2):
    print(f"[MATCHING] Возвращаем без совпадений/ошибка для {chat_id}")
    return level1 == level2 or (level1 in level2 or level2 in level1)

def get_average_feedback(user_id):
    conn = sqlite3.connect('chck.db', check_same_thread=False, timeout=10)
    cur = conn.cursor()
    cur.execute("""
        SELECT AVG(question1), AVG(question2), AVG(question3)
        FROM feedback
        WHERE to_user = ?
    """, (user_id,))
    result = cur.fetchone()
    conn.close()

    if result and all(r is not None for r in result):
        print(f"[MATCHING] Возвращаем без совпадений/ошибка для {chat_id}")
        return sum(result) / 3  # общий рейтинг
    print(f"[MATCHING] Возвращаем без совпадений/ошибка для {chat_id}")
    return None


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
    conn = sqlite3.connect('chck.db', check_same_thread=False, timeout=10)
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
    conn = sqlite3.connect('chck.db', check_same_thread=False, timeout=10)
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
        "Оцените собеседника по 5-балльной шкале в формате `5,4,5 Комментарий`:\n\n"
        "1) Насколько легко было общаться? (1–5)\n"
        "2) Насколько активным был собеседник? (1–5)\n"
        "3) Насколько дружелюбным был собеседник? (1–5)\n\n"
        "Пример:\n"
        "`5,4,5 Было здорово!`\n\n"
        "Если не было встречи — просто введите любой текст."
    )

    bot.send_message(chat_id, review_message)
    user_state[chat_id] = {'step': 'awaiting_feedback', 'partner_id': other_id}  # Ensure state is set

    bot.send_message(other_id, review_message)
    user_state[other_id] = {'step': 'awaiting_feedback', 'partner_id': chat_id}  # Ensure state is set

def schedule_review(chat_id1, chat_id2):
    review_time = (datetime.now(UTC) + timedelta(hours=48)).isoformat()
    conn = sqlite3.connect('chck.db', check_same_thread=False, timeout=10)
    cur = conn.cursor()
    cur.execute("INSERT INTO review_queue (chat_id1, chat_id2, send_time) VALUES (?, ?, ?)", (chat_id1, chat_id2, review_time))
    conn.commit()
    cur.close()
    conn.close()

def schedule_review_check():
    def checker():
        while True:
            from datetime import datetime, UTC
            now = datetime.now(UTC)
            conn = sqlite3.connect('chck.db', check_same_thread=False, timeout=10)
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
    print(f"[MATCHING] Начинаем подбор для {chat_id}")
    conn = sqlite3.connect('chck.db', check_same_thread=False, timeout=10)
    cur = conn.cursor()
    try:
        # ✅ Проверка, не находится ли пользователь уже в активной паре
        cur.execute("""
        SELECT match_time FROM matches 
        WHERE user_id = ? OR partner_id = ?""", (chat_id, chat_id))
        result = cur.fetchone()
        if result:
            match_time = datetime.fromisoformat(result[0]).replace(tzinfo=UTC)
            if datetime.now(UTC) < match_time + timedelta(hours=48):
                bot.send_message(chat_id, "⏳ Вы уже в паре. Подождите 48 часов до следующего собеседника.")
                print(f"[MATCHING] Возвращаем без совпадений/ошибка для {chat_id}")
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

        from datetime import datetime, UTC
        now = datetime.now(UTC)

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

        # Загружаем старые пары
        cur.execute("""SELECT user2 FROM past_matches WHERE user1 = ? UNION SELECT user1 FROM past_matches WHERE user2 = ?""", (chat_id, chat_id))
        already_matched = {row[0] for row in cur.fetchall()}

        current = user_data[chat_id]
        required_score = 2

        for user in users:
            other_id, name, age, level, gender, preferred_gender, username = user

            avg_feedback = get_average_feedback(other_id)
            if avg_feedback is not None and avg_feedback < 2.0:
                continue  # Пропускаем пользователей с плохими отзывами
            if current['preferred_gender'] != "Не важно" and gender != current['preferred_gender']:
                continue
            if preferred_gender != "Не важно" and current['gender'] != preferred_gender:
                continue

            match_score = 0
            if level_match(current['kazakh_level'], level):
                match_score += 1
            if age_overlap(current['age'], age):
                match_score += 1

            if other_id in already_matched:
                continue  # Уже были в паре

            if match_score >= required_score:
                # ✅ Совпадение найдено
                bot.send_message(chat_id, f"🎉 Вы совпали с @{username}!\n👤 Имя: {name}\n📅 Возраст: {age}\n⚧ Пол: {gender}\n🗣 Уровень казахского: {level}")
                bot.send_message(other_id, f"🎉 Вы совпали с @{current['telegram_username']}!\n👤 Имя: {current['name']}\n📅 Возраст: {current['age']}\n⚧ Пол: {current['gender']}\n🗣 Уровень казахского: {current['kazakh_level']}")

                match_time = datetime.now(UTC).isoformat()
                cur.execute("REPLACE INTO matches (user_id, partner_id, match_time) VALUES (?, ?, ?)", (chat_id, other_id, match_time))
                cur.execute("REPLACE INTO matches (user_id, partner_id, match_time) VALUES (?, ?, ?)", (other_id, chat_id, match_time))
                cur.execute("INSERT INTO past_matches (user1, user2, match_time) VALUES (?, ?, ?)", (chat_id, other_id, match_time))
                cur.execute("INSERT INTO past_matches (user1, user2, match_time) VALUES (?, ?, ?)", (other_id, chat_id, match_time))
                conn.commit()

                schedule_review(chat_id, other_id)
                cur.close()
                conn.close()
                print(f"[MATCHING] Возвращаем без совпадений/ошибка для {chat_id}")
                return

        # Если совпадений нет
        bot.send_message(chat_id, "😕 Пока нет совпадений. Попробуйте позже.")
        cur.close()
        conn.close()
    
    except Exception as e:
        print(f"[ERROR in find_match] {e}")

    finally:
        cur.close()
        conn.close()


# Запускаем проверку отзывов
schedule_review_check()
# Запускаем бота
import time

if __name__ == "__main__":
    schedule_review_check()
    create_tables_if_not_exist()
    bot.polling(none_stop=True) 