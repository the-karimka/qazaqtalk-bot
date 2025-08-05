import os
import traceback
import time
import threading
import sqlite3
import logging
from datetime import datetime, timezone, timedelta
from telebot import TeleBot, types
from flask import Flask, request, jsonify
from dotenv import load_dotenv

# --- Инициализация ---
load_dotenv()

# Конфигурация
DB_PATH = os.getenv('DATABASE_PATH', '/data/database.db')
BOT_TOKEN = os.getenv('BOT_TOKEN')
WEBHOOK_URL = os.getenv('WEBHOOK_URL')
PORT = int(os.getenv('PORT', 8080))

# Настройка логгирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

bot = TeleBot(BOT_TOKEN)
app = Flask(__name__)

# Глобальные переменные
user_data = {}
user_state = {}

# --- База данных ---
class Database:
    def __init__(self, db_path):
        self.db_path = db_path
        self.lock = threading.Lock()
        self._init_db()

    def _ensure_db_dir(self):
        """Создает папку для БД, если её нет"""
        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir)
            
    def _init_db(self):
        """Инициализация таблиц в базе данных"""
        tables = [
            '''CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                name TEXT,
                age TEXT,
                kazakh_level TEXT,
                gender TEXT,
                preferred_gender TEXT,
                telegram_username TEXT
            )''',
            '''CREATE TABLE IF NOT EXISTS matches (
                user1 INTEGER,
                user2 INTEGER,
                match_time TEXT,
                PRIMARY KEY (user1, user2)
            )''',
            '''CREATE TABLE IF NOT EXISTS feedback (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                from_user INTEGER,
                to_user INTEGER,
                question1 INTEGER,
                question2 INTEGER,
                question3 INTEGER,
                comment TEXT,
                timestamp TEXT
            )''',
            '''CREATE TABLE IF NOT EXISTS past_matches (
                user1 INTEGER,
                user2 INTEGER,
                match_time TEXT,
                PRIMARY KEY (user1, user2)
            )''',
            '''CREATE TABLE IF NOT EXISTS review_queue (
                chat_id1 INTEGER,
                chat_id2 INTEGER,
                send_time TEXT
            )'''
        ]

        with self._get_connection() as conn:
            cur = conn.cursor()
            for table in tables:
                cur.execute(table)
            conn.commit()
        logger.info(f"База данных инициализирована: {self.db_path}")

    def _get_connection(self):
        """Возвращает соединение с базой данных"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def execute(self, query, params=(), commit=False):
        """Безопасное выполнение SQL-запроса"""
        with self.lock:
            with self._get_connection() as conn:
                cur = conn.cursor()
                try:
                    cur.execute(query, params)
                    if commit:
                        conn.commit()
                    if query.strip().upper().startswith('SELECT'):
                        return cur.fetchall()
                    return True
                except Exception as e:
                    logger.error(f"Ошибка БД: {e}\nЗапрос: {query}\nПараметры: {params}")
                    conn.rollback()
                    raise

# Инициализация БД
db = Database(DB_PATH)

# --- Вспомогательные функции ---
def age_range_to_tuple(age_str):
    """Преобразует строку возраста в кортеж (min, max)"""
    if '+' in age_str:
        return (int(age_str.replace('+', '')), 99)
    return tuple(map(int, age_str.split('-')))

def age_overlap(age1, age2):
    """Проверяет пересечение возрастных диапазонов"""
    r1 = age_range_to_tuple(age1)
    r2 = age_range_to_tuple(age2)
    return max(r1[0], r2[0]) <= min(r1[1], r2[1])

def level_match(level1, level2):
    """Проверяет совместимость уровней языка"""
    LEVELS = ['Начинающий', 'Средний', 'Продвинутый', 'Носитель']
    return abs(LEVELS.index(level1) - LEVELS.index(level2)) <= 1

def get_average_feedback(user_id):
    """Возвращает средний рейтинг пользователя"""
    result = db.execute(
        "SELECT AVG(question1), AVG(question2), AVG(question3) FROM feedback WHERE to_user = ?",
        (user_id,)
    )
    if result and all(r is not None for r in result[0]):
        return sum(result[0]) / 3
    return None

def ask_question(chat_id, question, options):
    """Отправляет вопрос с inline-кнопками"""
    markup = types.InlineKeyboardMarkup()
    for option in options:
        markup.add(types.InlineKeyboardButton(option, callback_data=option))
    bot.send_message(chat_id, question, reply_markup=markup)

# --- Обработчики команд ---
@bot.message_handler(commands=['start'])
def start(message):
    """Обработчик команды /start"""
    try:
        username = message.from_user.username
        if not username:
            bot.send_message(message.chat.id, "Введите ваш Telegram username:")
            bot.register_next_step_handler(message, get_username)
            return

        user_data[message.chat.id] = {'telegram_username': username}
        bot.send_message(
            message.chat.id,
            "Сәлем! 👋 Добро пожаловать в QazaqTalk!\n\nВведите ваше имя:"
        )
        bot.register_next_step_handler(message, get_name)
    except Exception as e:
        logger.error(f"Ошибка в /start: {traceback.format_exc()}")
        bot.send_message(message.chat.id, "⚠️ Произошла ошибка. Попробуйте /start")

def get_username(message):
    """Получение username пользователя"""
    try:
        user_data[message.chat.id] = {'telegram_username': message.text.strip()}
        bot.send_message(message.chat.id, "Введите ваше имя:")
        bot.register_next_step_handler(message, get_name)
    except Exception as e:
        logger.error(f"Ошибка получения username: {traceback.format_exc()}")
        bot.send_message(message.chat.id, "⚠️ Ошибка. Попробуйте /start")

def get_name(message):
    """Получение имени пользователя"""
    try:
        user_data[message.chat.id]['name'] = message.text.strip()
        ask_question(
            message.chat.id,
            "Сколько вам лет?",
            ["10-13", "14-16", "17-20", "21-25", "30-35", "35+"]
        )
    except Exception as e:
        logger.error(f"Ошибка получения имени: {traceback.format_exc()}")
        bot.send_message(message.chat.id, "⚠️ Ошибка. Попробуйте /start")

@bot.callback_query_handler(func=lambda call: True)
def handle_callback(call):
    """Обработчик inline-кнопок"""
    try:
        chat_id = call.message.chat.id
        data = call.data

        if 'age' not in user_data.get(chat_id, {}):
            user_data[chat_id]['age'] = data
            ask_question(chat_id, "Ваш уровень казахского?", 
                        ["Начинающий", "Средний", "Продвинутый", "Носитель"])
        
        elif 'kazakh_level' not in user_data.get(chat_id, {}):
            user_data[chat_id]['kazakh_level'] = data
            ask_question(chat_id, "Ваш пол?", ["Мужской", "Женский"])
        
        elif 'gender' not in user_data.get(chat_id, {}):
            user_data[chat_id]['gender'] = data
            ask_question(chat_id, "С кем хотите практиковаться?", 
                        ["Мужской", "Женский", "Не важно"])
        
        elif 'preferred_gender' not in user_data.get(chat_id, {}):
            user_data[chat_id]['preferred_gender'] = data
            save_to_db(chat_id)

    except Exception as e:
        logger.error(f"Ошибка обработки callback: {traceback.format_exc()}")
        bot.send_message(chat_id, "⚠️ Ошибка. Попробуйте /start")

def save_to_db(chat_id):
    """Сохраняет данные пользователя в БД"""
    try:
        user = user_data[chat_id]
        db.execute(
            """INSERT OR REPLACE INTO users 
            (id, name, age, kazakh_level, gender, preferred_gender, telegram_username) 
            VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (chat_id, user['name'], user['age'], user['kazakh_level'], 
             user['gender'], user['preferred_gender'], user['telegram_username']),
            commit=True
        )
        find_match(chat_id)
    except Exception as e:
        logger.error(f"Ошибка сохранения в БД: {traceback.format_exc()}")
        bot.send_message(chat_id, "⚠️ Ошибка сохранения. Попробуйте /start")

# --- Система мэтчинга ---
def find_match(chat_id):
    """Поиск совместимого собеседника"""
    try:
        logger.info(f"Поиск пары для {chat_id}")
        
        # Проверка активных совпадений
        active_matches = db.execute(
            "SELECT user2, match_time FROM matches WHERE user1 = ?",
            (chat_id,)
        )
        
        if active_matches:
            match_time = datetime.fromisoformat(active_matches[0]['match_time'])
            if datetime.now(timezone.utc) < match_time + timedelta(hours=48):
                bot.send_message(chat_id, "⏳ У вас уже есть активная пара. Попробуйте позже.")
                return

        # Поиск совместимых пользователей
        current_user = db.execute(
            "SELECT * FROM users WHERE id = ?", 
            (chat_id,)
        )[0]

        exclude_users = {row['user1'] for row in db.execute(
            "SELECT user1 FROM matches"
        )}.union({chat_id})

        potential_matches = db.execute(
            """SELECT * FROM users 
            WHERE id NOT IN ({}) 
            AND preferred_gender IN (?, 'Не важно')
            AND gender IN (?, 'Не важно')""".format(','.join('?'*len(exclude_users))),
            [*exclude_users, current_user['gender'], current_user['preferred_gender']]
        )

        for match in potential_matches:
            if (level_match(current_user['kazakh_level'], match['kazakh_level']) and 
               age_overlap(current_user['age'], match['age'])):
                
                # Создаем пару
                match_time = datetime.now(timezone.utc).isoformat()
                db.execute(
                    "INSERT INTO matches (user1, user2, match_time) VALUES (?, ?, ?)",
                    (chat_id, match['id'], match_time),
                    commit=True
                )
                db.execute(
                    "INSERT INTO matches (user1, user2, match_time) VALUES (?, ?, ?)",
                    (match['id'], chat_id, match_time),
                    commit=True
                )
                
                # Отправляем уведомления
                bot.send_message(
                    chat_id,
                    f"🎉 Вы совпали с @{match['telegram_username']}!\n"
                    f"👤 Имя: {match['name']}\n"
                    f"📅 Возраст: {match['age']}\n"
                    f"⚧ Пол: {match['gender']}\n"
                    f"🗣 Уровень: {match['kazakh_level']}"
                )
                
                bot.send_message(
                    match['id'],
                    f"🎉 Вы совпали с @{current_user['telegram_username']}!\n"
                    f"👤 Имя: {current_user['name']}\n"
                    f"📅 Возраст: {current_user['age']}\n"
                    f"⚧ Пол: {current_user['gender']}\n"
                    f"🗣 Уровень: {current_user['kazakh_level']}"
                )
                
                # Планируем отзыв
                schedule_review(chat_id, match['id'])
                return

        bot.send_message(chat_id, "😕 Пока нет подходящих пар. Попробуйте позже.")

    except Exception as e:
        logger.error(f"Ошибка поиска пары: {traceback.format_exc()}")
        bot.send_message(chat_id, "⚠️ Ошибка поиска пары. Попробуйте позже.")

# --- Система отзывов ---
def schedule_review(chat_id1, chat_id2):
    """Планирует отправку запроса отзыва через 48 часов"""
    try:
        review_time = (datetime.now(timezone.utc) + timedelta(hours=48)).isoformat()
        db.execute(
            "INSERT INTO review_queue (chat_id1, chat_id2, send_time) VALUES (?, ?, ?)",
            (chat_id1, chat_id2, review_time),
            commit=True
        )
    except Exception as e:
        logger.error(f"Ошибка планирования отзыва: {traceback.format_exc()}")

def schedule_review_check():
    """Фоновая проверка очереди отзывов"""
    while True:
        try:
            now = datetime.now(timezone.utc)
            reviews = db.execute(
                "SELECT chat_id1, chat_id2, send_time FROM review_queue"
            )
            
            for review in reviews:
                if datetime.fromisoformat(review['send_time']) <= now:
                    send_review_request(review['chat_id1'], review['chat_id2'])
                    db.execute(
                        "DELETE FROM review_queue WHERE chat_id1 = ? AND chat_id2 = ?",
                        (review['chat_id1'], review['chat_id2']),
                        commit=True
                    )
            
            time.sleep(60)
        except Exception as e:
            logger.error(f"Ошибка проверки отзывов: {traceback.format_exc()}")
            time.sleep(300)

def send_review_request(chat_id, partner_id):
    """Отправляет запрос на отзыв"""
    try:
        message = (
            "Оцените собеседника (1-5):\n"
            "1) Легкость общения\n"
            "2) Активность\n"
            "3) Дружелюбие\n\n"
            "Формат: `5,4,5 Комментарий`"
        )
        
        bot.send_message(chat_id, message)
        user_state[chat_id] = {'step': 'awaiting_feedback', 'partner_id': partner_id}
        
        bot.send_message(partner_id, message)
        user_state[partner_id] = {'step': 'awaiting_feedback', 'partner_id': chat_id}
    
    except Exception as e:
        logger.error(f"Ошибка отправки отзыва: {traceback.format_exc()}")

@bot.message_handler(func=lambda m: user_state.get(m.chat.id, {}).get('step') == 'awaiting_feedback')
def process_feedback(message):
    """Обработка отзыва от пользователя"""
    try:
        chat_id = message.chat.id
        partner_id = user_state[chat_id]['partner_id']
        text = message.text.strip()

        # Проверка существующего отзыва
        existing = db.execute(
            "SELECT 1 FROM feedback WHERE from_user = ? AND to_user = ?",
            (chat_id, partner_id)
        )
        if existing:
            bot.send_message(chat_id, "⚠️ Вы уже оставили отзыв")
            return

        # Парсинг оценок
        parts = text.split(maxsplit=1)
        scores = list(map(int, parts[0].split(',')))
        if len(scores) != 3:
            raise ValueError("Нужно 3 оценки")
        
        comment = parts[1] if len(parts) > 1 else ""
        if any(s in [1,2] for s in scores) and not comment:
            bot.send_message(chat_id, "❗ При низкой оценке нужен комментарий")
            return

        # Сохранение отзыва
        db.execute(
            """INSERT INTO feedback 
            (from_user, to_user, question1, question2, question3, comment, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (chat_id, partner_id, *scores, comment, datetime.now(timezone.utc).isoformat()),
            commit=True
        )

        # Блокировка плохих пар
        if any(s <= 3 for s in scores):
            db.execute(
                "INSERT OR IGNORE INTO past_matches (user1, user2, match_time) VALUES (?, ?, ?)",
                (chat_id, partner_id, datetime.now(timezone.utc).isoformat()),
                commit=True
            )
            db.execute(
                "INSERT OR IGNORE INTO past_matches (user1, user2, match_time) VALUES (?, ?, ?)",
                (partner_id, chat_id, datetime.now(timezone.utc).isoformat()),
                commit=True
            )

        bot.send_message(chat_id, "✅ Отзыв сохранен")
        del user_state[chat_id]

    except Exception as e:
        logger.error(f"Ошибка обработки отзыва: {traceback.format_exc()}")
        bot.send_message(message.chat.id, "⚠️ Ошибка. Используйте формат: `5,4,5 Комментарий`")

# --- Webhook обработчики ---
@app.route('/' + BOT_TOKEN, methods=['POST'])
def webhook():
    """Endpoint для обработки webhook-запросов от Telegram"""
    if request.headers.get('content-type') == 'application/json':
        json_data = request.get_data().decode('utf-8')
        update = types.Update.de_json(json_data)
        bot.process_new_updates([update])
        return '', 200
    return 'Bad request', 400

@app.route('/')
def index():
    """Корневой endpoint для проверки работы сервера"""
    return 'QazaqTalk Bot is running!'

# --- Запуск приложения ---
if __name__ == '__main__':
    # Запуск фоновой проверки отзывов
    threading.Thread(target=schedule_review_check, daemon=True).start()
    
    # Установка webhook
    bot.remove_webhook()
    time.sleep(1)
    webhook_url = f"https://{WEBHOOK_URL}/{BOT_TOKEN}"
    bot.set_webhook(url=webhook_url)
    logger.info(f"Webhook установлен: {webhook_url}")
    
    # Запуск Flask приложения
    app.run(host='0.0.0.0', port=PORT)