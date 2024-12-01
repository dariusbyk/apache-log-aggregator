import re
import sqlite3
from flask import Flask, jsonify, request
import logging

app = Flask(__name__)


class LogEntry:
    def __init__(self, h: str = None, l: str = None, u: str = None, t: str = None, r: str = None, s: str = None, b: str = None):
        self.h = h
        self.l = l
        self.u = u
        self.t = t
        self.r = r
        self.s = s
        self.b = b

    def __repr__(self):
        return f'{self.h}, {self.l}, {self.u}, {self.t}, {self.r}, {self.s}, {self.b}'


def read_config(filename):
    with open(filename, 'r', encoding='UTF-8') as f:
        content = f.read()
    directory = re.search(r'directory\s*=\s*"(.*?)"', content)
    pattern_match = re.search(r'pattern\s*=\s*(.*)', content)
    if directory and pattern_match:
        directory = directory.group(1).replace('\\', '/')
        pattern = pattern_match.group(1)
        return directory, pattern
    raise ValueError("Не удалось найти ключи 'directory' или 'pattern' в файле конфигурации")


def setup_database():
    with sqlite3.connect('Parser.db') as con:
        cursor = con.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS logs (
                h TEXT,
                l TEXT,
                u TEXT,
                t TEXT,
                r TEXT,
                s TEXT,
                b TEXT,
                PRIMARY KEY (h, t, r)
            );
        """)
        con.commit()


def read_logs(directory, pattern):
    with open(directory, 'r', encoding='UTF-8') as f:
        lines = f.readlines()
    lines = [line.rstrip() for line in lines]
    return [LogEntry(*re.split(pattern, line)[1:-1]) for line in lines if re.match(pattern, line)]


def write_to_db(data):
    with sqlite3.connect('Parser.db') as con:
        cursor = con.cursor()
        inserted_count = 0
        try:
            for log in data:
                cursor.execute("""INSERT OR IGNORE INTO logs
                                  (h, l, u, t, r, s, b)
                                  VALUES (?, ?, ?, ?, ?, ?, ?);""",
                               (log.h, log.l, log.u, log.t, log.r, log.s, log.b))
                if cursor.rowcount > 0:
                    inserted_count += 1
            con.commit()
            print(f"Логи успешно обработаны. Количество новых строк: {inserted_count}")
        except sqlite3.Error as error:
            print(f"Ошибка при работе с SQLite: {error}")


@app.route('/logs', methods=['GET'])
def get_logs():
    query = request.args.get('query', '*')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    start_time = request.args.get('start_time')
    end_time = request.args.get('end_time')

    with sqlite3.connect('Parser.db') as con:
        cursor = con.cursor()
        try:
            if start_date and end_date and start_time and end_time:
                start_timestamp = f"{start_date}:{start_time}"
                end_timestamp = f"{end_date}:{end_time}"
                sql_query = f"""SELECT {query} FROM logs WHERE t BETWEEN ? AND ?;"""
                result = cursor.execute(sql_query, (start_timestamp, end_timestamp)).fetchall()
            else:
                sql_query = f"""SELECT {query} FROM logs;"""
                result = cursor.execute(sql_query).fetchall()

            logs = [dict(zip([column[0] for column in cursor.description], row)) for row in result]
            return jsonify(logs)
        except sqlite3.Error as error:
            return jsonify({'error': str(error)}), 500


def select_to_user():
    with sqlite3.connect('Parser.db') as con:
        cursor = con.cursor()
        try:
            print('h - IP address, l - Lengthy hostname of remote host, u - Remote user, t - Time of request, r - First request line, s - Final status, b - Size of response in bytes')
            query = input('Перечислите параметры, которые нужно вывести (через запятую): ').strip()
            if input('Вам нужен временной диапазон в вашем запросе? (Да/Нет): ').strip().lower() == 'да':
                start_date = input('Введите начальную дату (ДД/МММ/ГГГГ): ').strip()
                end_date = input('Введите конечную дату (ДД/МММ/ГГГГ): ').strip()
                start_time = input('Введите начальное время (ЧЧ:ММ:СС): ').strip()
                end_time = input('Введите конечное время (ЧЧ:ММ:СС): ').strip()

                start_timestamp = f"{start_date}:{start_time}"
                end_timestamp = f"{end_date}:{end_time}"

                ans = cursor.execute(
                    f"""SELECT {query} FROM logs WHERE
                        t BETWEEN ? AND ?;""",
                    (start_timestamp, end_timestamp)
                ).fetchall()
            else:
                ans = cursor.execute(f"""SELECT {query} FROM logs;""").fetchall()
            print("Результаты запроса:")
            for row in ans:
                print(row)
        except sqlite3.Error as error:
            print(f"Возникла ошибка при работе с SQLite: {error}")


if __name__ == '__main__':
    log = logging.getLogger('werkzeug')
    log.setLevel(logging.ERROR)
    setup_database()
    directory, pattern = read_config('cfg.txt')
    logs_arr = read_logs(directory, pattern)
    write_to_db(logs_arr)

    while True:
        mode = input("Вы хотите использовать консольный интерфейс или API? (console/api): ").strip().lower()
        if mode == 'console':
            select_to_user()
        elif mode == 'api':
            print("API запущен. Доступно по адресу: http://127.0.0.1:5000/logs")
            app.run(debug=False)
            break
        else:
            print("Неверный выбор. Попробуйте снова.")
