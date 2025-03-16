import psycopg2 # библиотека для работы с PostgreSQL в Python
import json # библиотека для работы с JSON-файлами
import csv # библиотека для работы c данными в формате CSV
import pandas as pd # библиотека для работы с данными в табличном формате (DataFrame), для загрузки и обработки данных из CSV и Excel-файлов
from sqlalchemy import create_engine # позволяет создавать соединение для загрузки данных из DataFrame в таблицы БД
from sqlalchemy import types # предоставляет типы данных для работы с таблицами БД
import os # предоставляет функции для работы с ОС, используется для проверки существования файлов, создания директорий и работы с путями
import shutil # для работы с файлами и директориями, здесь используется для перемещения файлов в архив
import time
from datetime import datetime # используется для получения текущего времени, форматирования дат и работы с временными интервалами

# Получаем конфигурацию БД 
def get_db_config():
    config_file = "db_config.json" # сохраняем конфигурацию в файл

    # Если файл конфигурации существует, загружаем данные из него
    if os.path.exists(config_file):
        with open(config_file, "r", encoding = "utf-8") as f:
            return json.load(f)
    else:
        # Если файла нет, запрашиваем данные у пользователя
        while True:
            try:
                print("Введите данные для подключения к БД PostgreSQL:")
                db_config = {
                    "database": input("Имя базы данных: ").strip(),
                    "user": input("Имя пользователя: ").strip(),
                    "password": input("Пароль: ").strip(),
                    "host": input("Хост: ").strip(),
                    "port": int(input("Порт: ").strip())  
                }       

                # Проверяем на пустые значения
                for key, value in db_config.items():
                    if not value:
                        raise ValueError(f"Поле '{key}' не может быть пустым.")

                # Проверяем подключение к БД
                try:
                    conn = psycopg2.connect(**db_config)
                    conn.close()
                except psycopg2.Error as e:
                    print(f"Ошибка при подключении к БД: {e}. Пожалуйста, повторите ввод.")
                    continue # Не сохраняем данные, запрашиваем снова  
                                
                # Сохраняем конфигурацию в файл
                with open(config_file, "w", encoding = "utf-8") as f:
                    json.dump(db_config, f, ensure_ascii = False, indent = 4) # не-ASCII символы оставляем как есть (не экранируем), задаем отступы
                
                return db_config
            
            except ValueError as e: # если введены некорректные данные или не все поля заполнены
                print(f"Ошибка: {e}. Пожалуйста, повторите ввод.")
            except psycopg2.Error as e: # если не удалось подключиться к БД или она недоступна
                print(f"Ошибка при подключении к БД: {e}. Пожалуйста, повторите ввод.")
            except Exception as e: # если ошибка другого типа
                print(f"Ошибка: {e}. Пожалуйста, повторите ввод.")

# Подключаемся к PostgreSQL
def connect_to_postgres():
    try:
        # Получаем конфигурацию БД
        db_config = get_db_config()

        conn = psycopg2.connect(**db_config) # устанавливаем соединение с БД PostgreSQL с параметрами db_config
        cursor = conn.cursor() # создаем курсор для выполнения SQL-запросов
                
        # Создаем движок SQLAlchemy для подключения к БД
        engine = create_engine(
            f'postgresql://{db_config["user"]}:{db_config["password"]}'
            f'@{db_config["host"]}:{db_config["port"]}/{db_config["database"]}'
        )

        print("Подключение к PostgreSQL установлено.")
        return conn, cursor, engine

    except psycopg2.Error as e:
        print(f"Ошибка при подключении к PostgreSQL: {e}")
        raise

# Создаем таблицу метаданных
def create_metadata_table(cursor, conn):
    try:
        # Создаем схему bank, если она не существует
        cursor.execute('''
            create schema if not exists bank;
        ''')
        conn.commit()

        # Создаем таблицу metadata в схеме bank
        cursor.execute('''
            create table if not exists bank.metadata (
                id serial primary key,
                process_name varchar(256),
                status varchar(128),
                start_time timestamp,
                end_time timestamp
            );
        ''')
        conn.commit()
    except Exception as e:
        print(f"Ошибка при создании таблицы metadata: {e}")
        raise

# Формируем словарь с путями к файлам на основе введенной пользователем даты
def get_file_set(date_str):
    file_set = {
        "transactions": f"data/transactions_{date_str}.txt", # путь к файлу с транзакциями
        "terminals": f"data/terminals_{date_str}.xlsx", # путь к файлу с терминалами
        "passport_blacklist": f"data/passport_blacklist_{date_str}.xlsx" # путь к файлу с черным списком паспортов
    }
    return file_set

# Проверяем, существуют ли файлы по указанным путям
def check_files_exist(file_set):
    for file_path in file_set.values(): # итерируемся по значениям словаря (путям к файлам)
        if not os.path.exists(file_path): # если файл не найден
            print(f"Файл {file_path} не найден.") # выводим сообщение об ошибке
            return False
    return True # если все файлы существуют, возвращаем True

# Выполняем SQL-запрос(ы) из файла
def execute_sql_file(cursor, conn, file_path):
    try:
        with open(file_path, 'r', encoding = 'utf-8') as sql_file: # открываем SQL-файл для чтения
            sql_commands = sql_file.read() # читаем содержимое файла в переменную
        cursor.execute(sql_commands) # выполняем SQL-запросы
        conn.commit() # фиксируем изменения в БД
        print(f"SQL-файл {file_path} успешно выполнен.")
    except Exception as e: # если ошибка, она перехватывается
        print(f"Ошибка при выполнении SQL-файла {file_path}: {e}") # и выводится сообщение об ошибке
        raise # прерываем выполнение функции, передаем управление вызывающему коду

# Преобразуем текстовый файл с разделителем ; в csv-файл с разделителем ,
def txt_to_csv(input_file, output_file):
    try:
        with open(input_file, 'r', encoding = 'utf-8') as infile, open(output_file, 'w', encoding='utf-8', newline='') as outfile:
            # если output_file существует, он будет перезаписан
            # newline = '' предотвращает добавление лишних пустых строк между записями в csv-файле
            reader = csv.reader(infile, delimiter = ';') # создаем итератор для разбиения строк на значения с помощью разделителя
            writer = csv.writer(outfile, delimiter = ',') # создаем объект для записи данных в csv-файл
            for row in reader: # каждую строку файла разбиваем на список значений на основе разделителя ;
                writer.writerow(row) # записываем строку в csv-файл, разделяя значения символом ,
        print(f"Файл {input_file} успешно преобразован в {output_file}.")
    except Exception as e:
        print(f"Ошибка при преобразовании файла {input_file}: {e}")
        raise

# Загружаем данные из файлов csv или xlsx в stg-таблицу
def csv2sql(engine, path, table_name, schema):
    try:
        abs_path = os.path.abspath(path) # преобразуем относительный путь в абсолютный

        # Читаем данные из файла
        if path.endswith('.csv'): # если путь заканчивается на .csv
            df = pd.read_csv(abs_path, sep = ',') # читаем csv-файл с разделителем , и загружаем данные в DataFrame
        elif path.endswith('.xlsx'): # если путь заканчивается на .xlsx
            df = pd.read_excel(abs_path) # читаем Excel-файл и загружаем данные в DataFrame
                
        # Корректируем данные в столбце amount
        if 'amount' in df.columns:
            df['amount'] = df['amount'].str.replace(',', '.').astype(float)
            # заменяем запятые на точки в значениях столбца amount
            # преобразует значения столбца amount в тип float

        # Загружаем данные в таблицу БД
        df.to_sql(
            name = table_name, # имя таблицы, в которую загружаем данные
            con = engine, # соединение с БД (движок SQLAlchemy)
            schema = schema, # схема БД, в которой находится таблица
            if_exists = 'append', # если таблица уже существует, данные будут добавлены в конец
            index = False, # индекс DataFrame не должен записываться в таблицу
            dtype = { # типы данных для каждого столбца
                'transaction_id': types.String(128),
                'amount': types.Float(),
                'card_num': types.String(128),
                'oper_type': types.String(128),
                'oper_result': types.String(128),
                'terminal': types.String(128),
                'terminal_id': types.String(128),
                'terminal_type': types.String(128),
                'terminal_city': types.String(128),
                'terminal_address': types.String(256),
                'date': types.Date(),
                'passport': types.String(128)
            }
        )

        print(f"Данные из файла {path} успешно загружены в таблицу {schema}.{table_name}.")
    except Exception as e:
        print(f"Ошибка при загрузке данных из файла {path}: {e}")
        raise

# Перемещаем файл в архив
def move_to_archive(file_path):
    archive_dir = os.path.join(os.path.dirname(__file__), 'archive') # создаем путь к архивной папке
    os.makedirs(archive_dir, exist_ok=True) # создаем архивную папку - если она уже существует, ошибка не возникает

    # Разделяет имя файла из пути на имя и расширение
    file_name, file_extension = os.path.splitext(os.path.basename(file_path))

    # Добавляем временную метку к имени файла
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S") # форматируем дату и время в строку формата ГГГГММДД_ЧЧММСС
    archive_file_name = f"{file_name}_{timestamp}{file_extension}" # формируем новое имя файла, добавляя временную метку
    archive_path = os.path.join(archive_dir, archive_file_name) # объединяем путь к папке архива и новое имя файла

    try:
        # Перемещаем файл в архив
        shutil.move(file_path, archive_path) # перемещаем файл из file_path в archive_path
        print(f"Файл {os.path.basename(file_path)} успешно перемещен в архив.")
    except Exception as e:
        print(f"Ошибка при перемещении файла {file_path}: {e}")
        raise

# Загружаем данные из файлов транзакции, терминалы, черный список паспортов в стейджинговые таблицы БД
def load_to_staging(cursor, conn, engine, file_set): # принимает словарь, содержащий пути к файлам с данными
    try:
        start_time = datetime.now() # фиксируем текущее время, чтобы измерить время выполнения функции

        # Очистка стейджинговой таблицы терминалов перед загрузкой новых данных
        cursor.execute('truncate table bank.stg_terminals;')
        conn.commit()

        # Загрузка данных в stg_transactions
        txt_to_csv(file_set["transactions"], file_set["transactions"].replace(".txt", ".csv"))
        csv2sql(engine, file_set["transactions"].replace(".txt", ".csv"), "stg_transactions", "bank")
        move_to_archive(file_set["transactions"]) # перемещаем исходный файл в архив
        move_to_archive(file_set["transactions"].replace(".txt", ".csv")) # перемещаем преобразованный csv-файл в архив

        # Загрузка данных в stg_terminals
        csv2sql(engine, file_set["terminals"], "stg_terminals", "bank")
        move_to_archive(file_set["terminals"])
        
        # Загрузка данных в stg_passport_blacklist
        csv2sql(engine, file_set["passport_blacklist"], "stg_passport_blacklist", "bank")
        move_to_archive(file_set["passport_blacklist"])

        print("Данные успешно загружены в staging-таблицы.")
        log_metadata(cursor, conn, "load_to_staging", "success", start_time, datetime.now())
    except Exception as e:
        log_metadata(cursor, conn, "load_to_staging", "failed", start_time, datetime.now(), error_message = str(e))
        print(f"Ошибка при загрузке данных в staging-таблицы: {e}")
        raise

# Записываем метаданные о выполнении процесса в таблицу metadata
def log_metadata(cursor, conn, process_name, status, start_time, end_time):
    try:
        cursor.execute('''
            insert into bank.metadata (process_name, status, start_time, end_time)
            -- SQL-запрос для вставки данных в таблицу metadata в схеме bank
            values (%s, %s, %s, %s); -- плейсхолдеры для значений, которые будут подставлены из кортежа
        ''', (process_name, status, start_time, end_time))
        conn.commit()
        print(f"Метаданные для процесса '{process_name}' успешно записаны.")
    except Exception as e:
        print(f"Ошибка при записи метаданных: {e}")
        raise

# ETL-процесс для конкретной даты
def etl_process(conn, cursor, engine, file_set):
    try:
        start_time = datetime.now()

        # Создаем таблицу метаданных
        create_metadata_table(cursor, conn)

        # Создаем схему bank, таблицы clients, accounts, cards
        execute_sql_file(cursor, conn, 'sql_scripts/create_cards_accounts_clients.sql')
        log_metadata(cursor, conn, "create_cards_accounts_clients", "success", start_time, datetime.now())

        # Создаем остальные таблицы
        execute_sql_file(cursor, conn, 'sql_scripts/create_bank_tables.sql')
        log_metadata(cursor, conn, "create_bank_tables", "success", start_time, datetime.now())

        # Загружаем данные в stg-таблицы
        load_to_staging(cursor, conn, engine, file_set)

        # Загружаем данные в хранилище
        execute_sql_file(cursor, conn, 'sql_scripts/load_to_warehouse.sql')
        log_metadata(cursor, conn, "load_to_warehouse", "success", start_time, datetime.now())

        # Обновляем историческую таблицу терминалов
        execute_sql_file(cursor, conn, 'sql_scripts/update_hist_terminals.sql')
        log_metadata(cursor, conn, "update_hist_terminals", "success", start_time, datetime.now())

        # Строим витрину мошеннических операций
        execute_sql_file(cursor, conn, 'sql_scripts/build_fraud_report.sql')
        log_metadata(cursor, conn, "build_fraud_report", "success", start_time, datetime.now())

        print("ETL-процесс успешно завершен.")
    except Exception as e:
        log_metadata(cursor, conn, "etl_process", "failed", start_time, datetime.now())
        print(f"Ошибка в ETL-процессе: {e}")

def main():
    # Получаем конфигурацию БД и подключаемся к PostgreSQL
    conn, cursor, engine = connect_to_postgres()

    # Вводим дату с клавиатуры
    date_str = input("Введите дату в формате DDMMYYYY (например, 01032021): ")

    # Проверяем, корректна ли введенная дата
    try:
        datetime.strptime(date_str, "%d%m%Y")
    except ValueError:
        print("Некорректный формат даты. Пожалуйста, введите дату в формате DDMMYYYY.")
        return

    # Формируем file_set
    file_set = get_file_set(date_str)

    # Проверяем существование файлов
    if not check_files_exist(file_set):
        print("Один или несколько файлов не найдены. Проверьте пути к файлам.")
        return

    # Запускаем ETL-процесс
    etl_process(conn, cursor, engine, file_set)

    # Закрываем соединение с БД
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()