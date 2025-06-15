import telebot
import sqlite3
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
import difflib

bot = telebot.TeleBot("7671940309:AAECI8c7p3loeQ8aFzZ0dEuyXw_OYwp3mR8")
user_data = {}

topic_options = ["Путешествия", "Спорт", "Музыка", "Казахская культура и традиции", "История", "Политика", "Образование", "Фильмы и сериалы", "Бизнес и экономика", "Еда и кулинария", "Повседневная жизнь", "Другое", "✅Отправить"]

def has_common_language(lang1, lang2):
    set1 = set(map(str.strip, lang1.lower().split(',')))
    set2 = set(map(str.strip, lang2.lower().split(',')))
    return not set1.isdisjoint(set2)

def fuzzy_match(str1, str2, threshold=0.7):
    return difflib.SequenceMatcher(None, str1.lower(), str2.lower()).ratio() >= threshold

@bot.message_handler(commands=['start'])
def start(message):
    conn = sqlite3.connect('chck.db')
    cur = conn.cursor()

    # Создаём таблицу с нужными колонками
    cur.execute('''CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY,name TEXT,password TEXT,age TEXT,country TEXT,native_language TEXT,kazakh_level TEXT,other_languages TEXT,learning_reason TEXT,topics TEXT,conversation_partner TEXT,opposite_gender TEXT,telegram_username TEXT,likes TEXT)''')

    conn.commit()
    cur.close()
    conn.close()

    username = message.from_user.username if message.from_user.username else None
    if username:
        user_data[message.chat.id] = {'telegram_username': username}
        bot.send_message(message.chat.id, "Сәлем!👋\n "
                                        "Добро пожаловать в QazaqTalk – бота для изучения казахского языка!\n "
                                        "Здесь тебя ждут увлекательные уроки, интерактивные задания и полезные упражнения, которые помогут легко и с удовольствием освоить казахский язык. \n"
                                        "Готов начать? Ендеше, бірге үйренейік! 🚀 \n"
                                        "Для начала давай пройдем регистрацию! Введи свое имя, чтобы я мог обращаться к тебе лично. 😊")
        bot.register_next_step_handler(message, get_name)
    else:
        bot.send_message(message.chat.id, "Введите ваш Telegram username:")
        bot.register_next_step_handler(message, get_username)

def get_username(message):
    user_data[message.chat.id] = {'telegram_username': message.text.strip()}
    bot.send_message(message.chat.id, "Введите ваше имя:")
    bot.register_next_step_handler(message, get_name)

def get_name(message):
    user_data[message.chat.id]['name'] = message.text.strip()
    bot.send_message(message.chat.id, "Введите пароль (если вы пользуетесь впервые, то придумайте):")
    bot.register_next_step_handler(message, get_password)

def get_password(message):
    user_data[message.chat.id]['password'] = message.text.strip()
    ask_question(message.chat.id, "Сколько вам лет?", ["10-13", "14-16", "17-20", "21-25", "30-35", "35+"])


def get_country(message):
    user_data[message.chat.id]['country'] = message.text.strip()
    bot.send_message(message.chat.id, "Какой ваш родной язык?")
    bot.register_next_step_handler(message, get_native_language)

def get_native_language(message):
    user_data[message.chat.id]['native_language'] = message.text.strip()
    ask_question(message.chat.id, "Какой у вас уровень казахского?",
                 ["Начинающий", "Средний", "Продвинутый", "Носитель"])

def get_other_languages(message):
    user_data[message.chat.id]['other_languages'] = message.text.strip()
    ask_question(message.chat.id, "Почему вы изучаете казахский?",
                 ["Для работы", "Для учебы", "Для общения", "Для переезда", "Просто интересно", "Другое"])

def save_learning_reason(chat_id, reason):
    user_data[chat_id]['learning_reason'] = reason
    user_data[chat_id]['topics'] = []

    # Отправляем сообщение и сохраняем ID
    selected = user_data[chat_id]['topics']
    markup = InlineKeyboardMarkup()
    for topic in topic_options[:-1]:
        label = topic
        markup.add(InlineKeyboardButton(text=label, callback_data=topic))
    markup.add(InlineKeyboardButton(text="✅ Отправить", callback_data="✅Отправить"))

    msg = bot.send_message(chat_id, "Выберите темы для разговора (3-5):", reply_markup=markup)
    user_data[chat_id]['topic_msg_id'] = msg.message_id

def show_topic_options(chat_id):
    selected = user_data[chat_id].get('topics', [])
    markup = InlineKeyboardMarkup()

    for topic in topic_options[:-1]:
        label = f"✅ {topic}" if topic in selected else topic
        markup.add(InlineKeyboardButton(text=label, callback_data=topic))

    markup.add(InlineKeyboardButton(text="✅ Отправить", callback_data="✅Отправить"))

    # Редактируем клавиатуру вместо отправки нового сообщения
    bot.edit_message_reply_markup(chat_id=chat_id, message_id=user_data[chat_id]['topic_msg_id'], reply_markup=markup)

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
        bot.send_message(chat_id, "Из какой вы страны?")
        bot.register_next_step_handler(call.message, get_country)

    elif 'kazakh_level' not in user_data[chat_id]:
        user_data[chat_id]['kazakh_level'] = data
        bot.send_message(chat_id, "Какие языки вы еще знаете?")
        bot.register_next_step_handler(call.message, get_other_languages)

    elif 'learning_reason' not in user_data[chat_id]:
        if data == "Другое":
            bot.send_message(chat_id, "Напишите свою причину изучения казахского:")
            bot.register_next_step_handler(call.message, lambda msg: save_learning_reason(chat_id, msg.text))
        else:
            save_learning_reason(chat_id, data)

    elif 'topics' in user_data[chat_id]:
        if data == "✅Отправить":
            selected = user_data[chat_id]['topics']
            if 3 <= len(selected) <= 5:
                save_to_db(chat_id)
            else:
                bot.answer_callback_query(call.id, "Выберите от 3 до 5 тем.", show_alert=True)
        else:
            topics = user_data[chat_id]['topics']
            if data in topics:
                topics.remove(data)
            else:
                if len(topics) < 5:
                    topics.append(data)
                else:
                    bot.answer_callback_query(call.id, "Можно выбрать максимум 5 тем.", show_alert=True)
            show_topic_options(chat_id)  # обновим кнопки

def save_to_db(chat_id):
    conn = sqlite3.connect('chck.db')
    cur = conn.cursor()
    cur.execute("""REPLACE INTO users (id, name, password, age, country, native_language, kazakh_level, other_languages, learning_reason, topics, conversation_partner, opposite_gender, telegram_username, likes) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '')""", (chat_id, user_data[chat_id]['name'], user_data[chat_id]['password'], user_data[chat_id]['age'],user_data[chat_id]['country'], user_data[chat_id]['native_language'], user_data[chat_id]['kazakh_level'],user_data[chat_id]['other_languages'], user_data[chat_id]['learning_reason'], ','.join(user_data[chat_id]['topics']),'', '', user_data[chat_id]['telegram_username']))
    conn.commit()
    cur.close()
    conn.close()
    find_match(chat_id)

def find_match(chat_id):
    conn = sqlite3.connect('chck.db')
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE id != ?", (chat_id,))
    users = cur.fetchall()
    cur.close()
    conn.close()

    current_user = user_data[chat_id]
    for user in users:
        match_score = 0
        required_score = 3  # порог совместимости

        if fuzzy_match(current_user['country'], user[4]):
            match_score += 1

        if fuzzy_match(current_user['native_language'], user[5]):
            match_score += 1

        if fuzzy_match(current_user['kazakh_level'], user[6]):
            match_score += 1

        if has_common_language(current_user['other_languages'], user[7]):
            match_score += 1

        # проверка на причину изучения
        if current_user['learning_reason'] == "Другое" or user[8] == "Другое":
            required_score -= 1  # понижаем порог
        else:
            if fuzzy_match(current_user['learning_reason'], user[8]):
                match_score += 1

        # проверка на совпадение тем
        user_topics = set(map(str.strip, user[9].lower().split(',')))
        current_topics = set(map(str.strip, current_user['topics']))

        shared = user_topics.intersection(current_topics)

        if len(current_topics) <= 2 or len(user_topics) <= 2:
            if shared:
                match_score += 1
        else:
            total = max(len(current_topics), len(user_topics))
            if len(shared) / total >= 0.5:
                match_score += 1

        if match_score >= required_score:
            match_message = (f"Вы совпали с @{user[12]}!\n"
                             f"Имя: {user[1]}\nВозраст: {user[3]}\nСтрана: {user[4]}\n"
                             f"Родной язык: {user[5]}\nУровень казахского: {user[6]}\n")
            bot.send_message(chat_id, match_message)
            bot.send_message(user[0], match_message.replace(f"@{user[12]}", f"@{current_user['telegram_username']}"))
            return

    bot.send_message(chat_id, "Пока нет совпадений, попробуйте позже.")

bot.polling(none_stop=True)
