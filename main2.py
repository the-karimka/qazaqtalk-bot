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
    
    def _get_connection(self):  # ✅ Correctly indented inside the class
        """Возвращает соединение с базой данных"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn
    
    def _ensure_db_dir(self):
        """Создает папку для БД, если её нет"""
        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir)
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

    def add_missing_columns(self):
        """Добавляет отсутствующие столбцы в таблицы"""
        try:
            # Проверяем существование столбца rating
            cols = self.execute("PRAGMA table_info(users)")
            if cols and isinstance(cols, list):  # Check if we got results
                if not any(c[1] == 'rating' for c in cols):  # Column name is at index 1
                    self.execute(
                        "ALTER TABLE users ADD COLUMN rating REAL DEFAULT 3.0",
                        commit=True
                    )
                    logger.info("Добавлен столбец rating в таблицу users")
        except Exception as e:
            logger.error(f"Ошибка проверки/добавления столбцов: {e}")
            raise

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
            )''',  
            '''CREATE INDEX IF NOT EXISTS idx_feedback_to_user ON feedback(to_user)''',
            '''CREATE INDEX IF NOT EXISTS idx_matches_user1 ON matches(user1)''',
            '''CREATE INDEX IF NOT EXISTS idx_matches_user2 ON matches(user2)'''
        ]
        
        with self._get_connection() as conn: 
            cur = conn.cursor()
            for table in tables:
                cur.execute(table)
            conn.commit()
        logger.info(f"База данных инициализирована: {self.db_path}")

# Инициализация БД
db = Database(DB_PATH)
db.add_missing_columns()

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

def update_user_rating(user_id):
    """Обновляет рейтинг пользователя в таблице users"""
    try:
        avg_rating = db.execute(
            "SELECT AVG((question1 + question2 + question3) / 3.0) FROM feedback WHERE to_user = ?",
            (user_id,)
        )[0][0]
        if avg_rating is not None:  # Явная проверка на None
            db.execute(
            "SELECT AVG((question1 + question2 + question3) / 3.0) FROM feedback WHERE to_user = ?",
            (user_id,)
        )
        
        if avg_rating:
            db.execute(
                "UPDATE users SET rating = ? WHERE id = ?",
                (round(avg_rating, 2), user_id),
                commit=True
            )
    except Exception as e:
        logger.error(f"Ошибка обновления рейтинга для {user_id}: {e}")

def ask_question(chat_id, question, options):
    """Отправляет вопрос с inline-кнопками"""
    markup = types.InlineKeyboardMarkup()
    for option in options:
        markup.add(types.InlineKeyboardButton(option, callback_data=option))
    bot.send_message(chat_id, question, reply_markup=markup)

# Объединённый обработчик start/restart
def start_registration(message, is_restart=False):
    """Общая функция для начала регистрации"""
    try:
        chat_id = message.chat.id
        
        # Более надежная очистка данных при рестарте
        if is_restart:
            try:
                with db.lock:
                    db.execute("DELETE FROM users WHERE id = ?", (chat_id,), commit=True)
                    db.execute("DELETE FROM matches WHERE user1 = ? OR user2 = ?", 
                             (chat_id, chat_id), commit=True)
                    # Очищаем кэшированные данные
                    user_data.pop(chat_id, None)
                    user_state.pop(chat_id, None)
            except Exception as e:
                logger.error(f"Ошибка очистки данных при restart: {traceback.format_exc()}")
                raise

        # Проверяем username более надежно
        username = getattr(message.from_user, 'username', None)
        if not username or not username.strip():
            msg = ("🔁 Анкета сброшена. Введите ваш Telegram username (должен начинаться с @):" 
                  if is_restart else "Введите ваш Telegram username (должен начинаться с @):")
            sent_msg = bot.send_message(chat_id, msg)
            bot.register_next_step_handler(sent_msg, get_username)
            return

        # Инициализация/сброс данных пользователя
        user_data[chat_id] = {'telegram_username': username.strip('@')}
        
        greeting = "🔁 Анкета сброшена. Давайте начнем заново!\n\n" if is_restart else ""
        sent_msg = bot.send_message(
            chat_id,
            f"{greeting}Сәлем! 👋 Добро пожаловать в QazaqTalk!\n\nВведите ваше имя:"
        )
        bot.register_next_step_handler(sent_msg, get_name)
        
    except Exception as e:
        logger.error(f"Ошибка в {'/restart' if is_restart else '/start'}: {traceback.format_exc()}")
        bot.send_message(chat_id, "⚠️ Произошла ошибка. Попробуйте снова через /start")

@bot.message_handler(commands=['start'])
def handle_start(message):
    """Обработчик команды /start"""
    start_registration(message)

@bot.message_handler(commands=['restart'])
def handle_restart(message):
    """Обработчик команды /restart"""
    start_registration(message, is_restart=True)

@bot.message_handler(commands=['guidebook'])
def send_guidebook(message):
    """Улучшенный обработчик команды /guidebook"""
    try:
        chat_id = message.chat.id
        guidebook_path = 'guidebook.docx'
        
        # Проверяем доступность файла
        if not os.path.exists(guidebook_path):
            logger.warning(f"Файл гайдбука не найден: {guidebook_path}")
            bot.send_message(
                chat_id,
                "📚 Гайдбук временно недоступен. Администратор уже уведомлен о проблеме."
            )
            return

        # Проверяем размер файла
        file_size = os.path.getsize(guidebook_path) / (1024 * 1024)  # в MB
        if file_size > 50:  # Telegram ограничивает 50MB для ботов
            logger.error(f"Файл гайдбука слишком большой: {file_size:.2f}MB")
            bot.send_message(
                chat_id,
                "⚠️ Файл гайдбука слишком большой. Мы работаем над этим."
            )
            return

        # Отправляем файл с обработкой возможных ошибок
        with open(guidebook_path, 'rb') as f:
            bot.send_chat_action(chat_id, 'upload_document')
            bot.send_document(
                chat_id=chat_id,
                document=f,
                caption="📘 QazaqTalk Guidebook",
                timeout=30,
                visible_file_name="QazaqTalk_Guide.docx"  # Красивое имя файла
            )
            
    except Exception as e:
        logger.error(f"Ошибка отправки гайдбука: {traceback.format_exc()}")
        bot.send_message(
            chat_id,
            "⚠️ Произошла непредвиденная ошибка при отправке гайдбука. Попробуйте позже."
        )

@bot.message_handler(func=lambda message: True)
def handle_all_messages(message):
    """Обработка всех остальных сообщений"""
    bot.send_message(message.chat.id, "Спасибо за сообщение! Пожалуйста, используйте /start для начала работы.")

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

        if not current_user:
                bot.send_message(chat_id, "❌ Ваш профиль не найден. Пожалуйста, пройдите регистрацию снова.")
                return
        
        exclude_users = {row['user1'] for row in db.execute(
            "SELECT user1 FROM matches"
        )}.union({chat_id})

        # Получаем средний рейтинг текущего пользователя
        current_rating = get_average_feedback(chat_id) or 3.0  # 3.0 - дефолтный рейтинг
        
        # Исключаем пользователей из past_matches
        exclude_users |= {row['user2'] for row in db.execute(
            "SELECT user2 FROM past_matches WHERE user1 = ?",
            (chat_id,)
        )}
        
        potential_matches = db.execute(
    """SELECT 
        u.id, u.name, u.age, u.kazakh_level, 
        u.gender, u.preferred_gender, u.telegram_username,
        COALESCE(f.avg_rating, 3.0) as rating
    FROM users u
    LEFT JOIN (
        SELECT 
            to_user, 
            (AVG(question1) + AVG(question2) + AVG(question3)) / 3.0 as avg_rating
        FROM feedback 
        GROUP BY to_user
    ) f ON u.id = f.to_user
    WHERE u.id NOT IN ({}) 
    AND u.preferred_gender IN (?, 'Не важно')
    AND u.gender IN (?, 'Не важно')
    ORDER BY ABS(COALESCE(f.avg_rating, 3.0) - ?) ASC
    LIMIT 50""".format(','.join('?'*len(exclude_users))),
    [*exclude_users, current_user['gender'], current_user['preferred_gender'], current_rating]
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
                schedule_review(chat_id, match['id'])  # Для первого пользователя
                schedule_review(match['id'], chat_id)  # Для второго пользователя
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
    "SELECT chat_id1, chat_id2, send_time FROM review_queue "
    "WHERE send_time <= ? ORDER BY send_time LIMIT 100",
    (now.isoformat(),)
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

# Улучшенная send_review_request
def send_review_request(chat_id, partner_id):
    """Отправляет запрос на отзыв"""
    try:
        partner_info = db.execute(
            "SELECT name, telegram_username FROM users WHERE id = ?", 
            (partner_id,)
        )
        
        if not partner_info or len(partner_info) == 0:
            logger.error(f"Данные партнёра {partner_id} не найдены")
            return
        partner = partner_info[0]

        message = (
            f"📝 *Время оставить отзыв о вашей практике с {partner['name']} (@{partner['telegram_username']})*\n\n"
            "Пожалуйста, оцените:\n"
            "1️⃣ Легкость общения (1-5)\n"
            "2️⃣ Активность (1-5)\n"
            "3️⃣ Дружелюбие (1-5)\n\n"
            "*Формат:* `оценка1, оценка2, оценка3 [комментарий]`\n"
            "Пример: `5, 4, 5 Отличная практика!`\n\n"
            "Если не общались - отправьте `0`"
        )
        
        markup = types.ForceReply(selective=False)
        msg = bot.send_message(
            chat_id, 
            message, 
            parse_mode='Markdown',
            reply_markup=markup
        )
        
        user_state[chat_id] = {
            'step': 'awaiting_feedback',
            'partner_id': partner_id,
            'message_id': msg.message_id  # Сохраняем ID сообщения для возможного редактирования
        }
    
    except Exception as e:
        logger.error(f"Ошибка отправки отзыва: {traceback.format_exc()}")
        bot.send_message(chat_id, "⚠️ Не удалось отправить запрос отзыва")

# Улучшенная process_feedback
@bot.message_handler(func=lambda m: user_state.get(m.chat.id, {}).get('step') == 'awaiting_feedback')
def process_feedback(message):
    """Обработка отзыва от пользователя"""
    try:
        chat_id = message.chat.id
        state = user_state.get(chat_id, {})
        
        if not state or 'partner_id' not in state:
            bot.send_message(chat_id, "⚠️ Сессия устарела. Начните заново.")
            return

        partner_id = state['partner_id']

        is_valid_pair = db.execute(
            "SELECT 1 FROM matches WHERE user1 = ? AND user2 = ?",
            (chat_id, partner_id)
        )
        if not is_valid_pair or len(is_valid_pair) == 0:
            bot.send_message(chat_id, "❌ Нельзя оставить отзыв этому пользователю")
            if chat_id in user_state:
                del user_state[chat_id]
            return
        
        text = message.text.strip()

        # Обработка отмены
        if text == '0':
            bot.send_message(chat_id, "✅ Спасибо, мы учтём что встреча не состоялась")
            del user_state[chat_id]
            return

        # Валидация и парсинг
        parts = text.split(maxsplit=1)
        if len(parts[0].split(',')) != 3:
            raise ValueError("Нужно 3 оценки через запятую")

        scores = [int(s.strip()) for s in parts[0].split(',')]
        if any(s < 1 or s > 5 for s in scores):
            raise ValueError("Оценки должны быть от 1 до 5")

        comment = parts[1] if len(parts) > 1 else None
        if any(s <= 2 for s in scores) and not comment:
            raise ValueError("Для низких оценок нужен комментарий")

        # Сохранение в БД
        db.execute(
    """INSERT INTO feedback 
    (from_user, to_user, question1, question2, question3, comment, timestamp) 
    VALUES (?, ?, ?, ?, ?, ?, ?)""",
    (chat_id, partner_id, *scores, comment, datetime.now(timezone.utc).isoformat()),
    commit=True
)
        update_user_rating(partner_id)

        # Уведомление
        bot.send_message(chat_id, "✅ Спасибо за ваш отзыв!")
        
        # Очистка состояния
        if chat_id in user_state:
            del user_state[chat_id]

    except ValueError as ve:
        logger.warning(f"Некорректный отзыв: {ve}")
        bot.send_message(chat_id, f"⚠️ {ve}\nПожалуйста, используйте правильный формат")
    except Exception as e:
        logger.error(f"Ошибка обработки отзыва: {traceback.format_exc()}")
        bot.send_message(chat_id, "⚠️ Ошибка обработки. Попробуйте позже")
        if chat_id in user_state:
            del user_state[chat_id]

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
    try:
        # 1. Запуск фоновых процессов
        threading.Thread(target=schedule_review_check, daemon=True).start()
        
        # 2. Настройка webhook
        logger.info("Настройка webhook...")
        bot.remove_webhook()
        time.sleep(2)  # Увеличенная задержка для надежности
        
        webhook_url = f"https://{WEBHOOK_URL}/{BOT_TOKEN}"
        if not WEBHOOK_URL:
            logger.error("WEBHOOK_URL не установлен!")
            raise ValueError("WEBHOOK_URL не установлен")
            
        bot.set_webhook(
            url=webhook_url,
            max_connections=50,
            allowed_updates=["message", "callback_query"]
        )
        logger.info(f"Webhook установлен на: {webhook_url}")
        
        # 3. Запуск сервера
        logger.info(f"Запуск сервера на порту {PORT}...")
        
        # Для production используем Waitress
        from waitress import serve
        serve(
            app,
            host='0.0.0.0',
            port=PORT,
            threads=4,
            url_scheme='https'
        )
        
    except KeyboardInterrupt:
        logger.info("Бот остановлен вручную")
    except Exception as e:
        logger.error(f"Критическая ошибка: {str(e)}")
        raise
