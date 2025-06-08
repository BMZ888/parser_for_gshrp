# src/dwh_builder/upload_excel.py

import pandas as pd
import sqlite3
import sys
import os
from datetime import datetime
import glob # Библиотека для поиска файлов по шаблону

# --- НАСТРОЙКА ПУТЕЙ ДЛЯ ИМПОРТОВ ---
# Добавляем корень проекта в путь, чтобы можно было импортировать database
# Этот трюк работает, потому что upload_excel.py лежит на 2 уровня ниже корня
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, BASE_DIR)

from src.common import database

# --- КОНФИГУРАЦИЯ ---
# Папка, куда нужно класть Excel-файлы для загрузки
ESTIMATES_FOLDER = os.path.join(BASE_DIR, 'data', 'estimates')

def find_header_row(file_path: str) -> int:
    """
    Находит номер строки, в которой находится заголовок таблицы.
    Это нужно, чтобы пропустить "шапку" в файлах смет.
    """
    try:
        # Читаем первые 30 строк для надежности и ищем ключевые слова в заголовках
        df_header_check = pd.read_excel(file_path, header=None, nrows=30, engine='openpyxl')
        for i, row in df_header_check.iterrows():
            row_as_string = ' '.join(map(str, row.values))
            if 'Код ресурса, услуги' in row_as_string and 'Наименование строительного ресурса' in row_as_string:
                return i
    except Exception as e:
        print(f"  - Не удалось прочитать шапку файла {os.path.basename(file_path)}: {e}")
    return None

def process_excel_file(file_path: str, conn: sqlite3.Connection) -> int:
    """Обрабатывает один Excel-файл и загружает его в базу данных."""
    filename = os.path.basename(file_path)
    print(f"\n--- Обработка файла: {filename} ---")
    
    header_row_index = find_header_row(file_path)
    if header_row_index is None:
        print("  - Ошибка: Не удалось найти строку с заголовками в Excel. Файл пропущен.")
        return 0

    try:
        df = pd.read_excel(file_path, header=header_row_index, engine='openpyxl')
        
        columns_map = {
            'Код ресурса, услуги': 'item_code',
            'Наименование строительного ресурса, услуги': 'item_name',
            'Единица измерения': 'unit',
            'Сметная цена в текущем уровне цен, руб.': 'price_per_unit'
        }

        if not all(col in df.columns for col in columns_map.keys()):
            print(f"  - Ошибка: в файле отсутствуют необходимые колонки ({', '.join(columns_map.keys())}). Файл пропущен.")
            return 0

        df = df.rename(columns=columns_map)
        df = df[list(columns_map.values())] # Оставляем только нужные колонки
        
        df.dropna(subset=['item_code', 'price_per_unit'], how='all', inplace=True)
        df['price_per_unit'] = pd.to_numeric(df['price_per_unit'], errors='coerce')
        df.dropna(subset=['price_per_unit'], inplace=True)
        
        df['upload_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df['source_file'] = filename
        
        df.to_sql('fact_estimates', conn, if_exists='append', index=False)
        
        print(f"  - Успешно загружено {len(df)} строк.")
        return len(df)
        
    except Exception as e:
        print(f"  - Произошла ошибка при обработке файла: {e}")
        return 0

def init_estimates_table(conn: sqlite3.Connection):
    """Создает таблицу для хранения смет, если она еще не существует."""
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS fact_estimates (
        estimate_key INTEGER PRIMARY KEY AUTOINCREMENT,
        upload_date TIMESTAMP NOT NULL,
        source_file TEXT NOT NULL,
        item_code TEXT,
        item_name TEXT,
        unit TEXT,
        price_per_unit REAL
    )
    ''')
    conn.commit()
    print("Проверено/создано: таблица 'fact_estimates' для хранения смет готова.")

def main():
    """Главная функция для поиска и загрузки всех смет из папки."""
    print("="*50)
    print("--- Запуск модуля загрузки смет из Excel ---")
    
    os.makedirs(ESTIMATES_FOLDER, exist_ok=True)
    
    excel_files = glob.glob(os.path.join(ESTIMATES_FOLDER, '*.xlsx')) + \
                  glob.glob(os.path.join(ESTIMATES_FOLDER, '*.xls'))
                  
    if not excel_files:
        print(f"Excel-файлы не найдены в папке: {ESTIMATES_FOLDER}")
        print("Положите файлы смет в эту папку и запустите скрипт снова.")
        return

    db_path = database.get_dds_db_path()
    conn = database.get_db_connection(db_path)
    
    # 1. Гарантируем, что таблица существует
    init_estimates_table(conn)
    
    # 2. Обрабатываем каждый найденный файл
    total_rows_uploaded = 0
    for file in excel_files:
        total_rows_uploaded += process_excel_file(file, conn)

    conn.close()
    print("="*50)
    print(f"Загрузка завершена. Всего добавлено {total_rows_uploaded} строк в таблицу 'fact_estimates'.")

if __name__ == "__main__":
    main()
