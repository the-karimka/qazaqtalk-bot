import telebot
import sqlite3

bot = telebot.TeleBot('7671940309:AAFIjRq8aYaMh-OjO8YuL6N6l1DV3e4hMyo')

@bot.message_handler(commands=['start'])
def start(message):
    conn = sqlite3.connect('chck.db')
    cur = conn.cursor()

    cur.execute('CREATE TABLE IF NOT EXISTS users (id int auto_increment primary key, name varchar(50), pass varchar(50)')
    conn.commit()
    cur.close()
    conn.close()

    bot.send_message(message.chat,id, 'Сәлем!👋 Добро пожаловать в QazUp – бота для изучения казахского языка! Здесь тебя ждут увлекательные уроки, интерактивные задания и полезные упражнения, которые помогут легко и с удовольствием освоить казахский язык. Готов начать? Ендеше, бірге үйренейік! 🚀 Для начала давай пройдем регистрацию! Введи свое имя, чтобы я мог обращаться к тебе лично. 😊')


bot.polling(none_stop=True)