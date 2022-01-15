import os
import mysql.connector
from contextlib import contextmanager, closing
import requests
from datetime import datetime
import time
import contextlib
from mysql.connector import connect, Error
from pathlib import Path
import yaml
from dotenv import load_dotenv

load_dotenv()

conn = mysql.connector.connect(
    host=os.getenv('MYSQL_HOST'),
    database=os.getenv('MYSQL_DB'),
    user=os.getenv('MYSQL_USER'),
    password=os.getenv('MYSQL_PASSWORD')
)


@contextmanager
def connect_to_database():
    with contextlib.closing(conn.cursor()) as cursor:
        try:
            yield cursor
        except Error:
            conn.rollback()
        else:
            conn.commit()


def create_tables():
    with connect_to_database() as cursor:
        cursor.execute('PRAGMA foreign_keys=ON')
        cursor.execute("""CREATE TABLE toys
                        (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        toy_id INT NOT NULL,
                        name TEXT NOT NULL,
                        status TEXT NOT NULL,
                        status_updated DATE NOT NULL
                        )
                        """)    # Task 1
        cursor.execute("""CREATE TABLE games
                        (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        games_id INT NOT NULL,
                        name TEXT NOT NULL,
                        date DATE NOT NULL,
                        )
                        """)    # Task 1
        cursor.execute("""CREATE TABLE toys_games
                        (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        games_id INTEGER NOT NULL,
                        toy_id INTEGER NOT NULL,
                        note LONGBLOB NOT NULL,
                        FOREIGN KEY (games_id) REFERENCES games (id) ON DELETE CASCADE,
                        FOREIGN KEY (toy_id) REFERENCES toys (id) ON DELETE CASCADE
                        )
                        """)    # Task 1
        cursor.execute("""CREATE TABLE toys_repair
                        (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        toy_id INTEGER NOT NULL,
                        issue_description LONGBLOB,
                        FOREIGN KEY (toy_id) REFERENCES toys (id) ON DELETE CASCADE
                        )
                        """)    # Task 2


def open_config():
    with open(Path('conf/conf_url.yaml'), 'r') as config:
        return yaml.safe_load(config)


def get_data():
    with open(Path('date.txt')) as file:
        date_off = file.read()
    with open(Path('date_to_insert.txt'), "w") as f:
        f.write(date_off)
    param = open_config()
    response = requests.get('http://example.com/users?date={current_date}&prev_date={date_with_offset}'.format(
        current_date=datetime.now().date(), date_with_offset=date_off))
    with open(Path('date.txt'), "w") as file:
        curr_date = str(datetime.now().date())
        file.write(curr_date)
    return response


def open_toys():
    with open(Path('a.yaml'), 'r') as toys:
        return yaml.safe_load(toys)


def open_games():
    with open(Path('b.yaml'), 'r') as games:
        return yaml.safe_load(games)


def insert_toys_to_table():
    with connect_to_database() as cursor:
        data_toys = open_toys()
        for i in data_toys['toys']:
            cursor.execute(
                "INSERT INTO toys(toy_id, name, status, status_updated) VALUES (?, ?, ?, ?)",
                (i['id'], i['name'], i['status'], i['status_updated']))
            for j in i['games']:
                cursor.execute(
                    "INSERT INTO toys_games(games_id, toy_id, note) VALUES (?, ?, ?)",
                    (j['id'], i['id'], j['note']))


def insert_games_to_table():
    with connect_to_database() as cursor:
        data_games = open_games()
        for i in data_games['games']:
            cursor.execute(
                "INSERT INTO games(games_id, name, date) VALUES (?, ?, ?)",
                (i['id'], i['name'], i['date']))


def insert_data_to_table_toys_repair():
    with connect_to_database() as cursor:
        cursor.execute(
            "INSERT INTO toys_repair(toy_id) SELECT id FROM toys WHERE status IN ('repair', 'break', 'broken')")


def open_two_dates():
    new_date = str(datetime.now().date())
    with open(Path('date_to_insert.txt'), "r") as file:
        old_date = file.read()
    return old_date, new_date


def return_data_games():    # Task 3.a
    with connect_to_database() as cursor:
        from_date, to_date = open_two_dates()
        cursor.execute(
            'SELECT * FROM games WHERE date BETWEEN ? AND ?',
            (from_date, to_date))
        data_from_games = cursor.fetchall()
        return data_from_games


def return_data_toys():    # Task 3.b
    with connect_to_database() as cursor:
        from_date, to_date = open_two_dates()
        cursor.execute(
            'SELECT * FROM toys WHERE status_updated BETWEEN ? AND ?',
            (from_date, to_date))
        data_from_toys = cursor.fetchall()
        return data_from_toys


def return_data_from_table_toys_repair():    # Task 3.c
    with connect_to_database() as cursor:
        cursor.execute(
            'SELECT note FROM toys_repair, toys_games WHERE toys_repair.toy_id = toys_repair.toy_id'
        )
        data_from_toys_repair = cursor.fetchall()
        with open(Path('c.yaml'), "w") as f:
            documents = yaml.dump(data_from_toys_repair, f)
        return data_from_toys_repair


def return_all_data_from_tables():    # Task 4
    with connect_to_database() as cursor:
        cursor.execute(
            'SELECT toy_id, toys.name, status, status_updated, games.name, date, note FROM toys, games, toys_games WHERE date BETWEEN ? AND ?',
            (str(datetime.now().replace(year=datetime.now().date().year-1).date()), str(datetime.now().date())))
        all_data_from_tables = cursor.fetchall()
        return all_data_from_tables


def return_data_toys_which_never_repair():
    with connect_to_database() as cursor:
        cursor.execute(
            "SELECT toy_id FROM toys WHERE status NOT IN ('repair', 'break', 'broken')")
        data_toys_never_repair = cursor.fetchall()
        return data_toys_never_repair


def main():
    create_tables()
    with open(Path('date.txt'), "w") as f:
        v = str(datetime.now().date())
        f.write(v)
    while True:
        #        data_from_url = get_data()    ниже в комментариях укажу, почему функция в комментах
        insert_toys_to_table()    # from data_from_url
        insert_games_to_table()    # from data_from_url
        insert_data_to_table_toys_repair()
        insert_data_to_table_toys_repair()
        print(return_data_games())
        print(return_data_toys())
        print(return_data_from_table_toys_repair())
        print(return_all_data_from_tables())
        print(return_data_toys_which_never_repair())
        time.sleep(60*60*24*7)
    connect.close()


if __name__ == '__main__':
    main()

"""
Всё-таки не смог до конца разобраться в задании. Ранее приходилось работать с парсингом API. Но с передачей 
параметров не сталкивался. А попытаться разобраться не получилось по той причине, что ссылка учебная. Так же, как 
я понял из задания, я должен отправлять запрос с параметрами, с помощью которых у меня должны вставлять данные сразу 
в БД. С таким ранее я тоже не сталкивался. И было бы интересно разобраться, но крайне ограничен по времени. Позже 
обязательно разберусь самостоятельно.
Вторая проблема, с которой я столкнулся, я не совсем понял контекст формата yaml. Или он представлен в задании, 
чтобы мне было проще понять, в каком формате я получаю API, или я в таком формате должен сохранять данные, после 
получения по API. Но опять же, сохранить данные в таком формате мне было сложно разобраться по причине того, что 
ссылка была учебной.
Потому для вставки в БД я использовал сами эти файлы, которые были даны в задании, предварительно их сохранив.
Следующая проблема, с которой я столкнулся- это написание SQL запросов. Опыт в написании raw запросов имею. Но опыт 
сводился к написанию простых запросов. Более сложные приходилось писать через Django ORM, работая с PostgreSQL. 
Потому, не исключено, что работают они неверно.
Хотел лишь обозначить с какими проблема я столкнулся для Вашей объективной оценки. Спасибо. 

"""
