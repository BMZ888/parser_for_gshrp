# code/database.py

import sqlite3
import os

# --- КОНФИГУРАЦИЯ ПУТЕЙ ---

# Определяем базовую директорию проекта (на один уровень выше папки code)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Определяем пути к папкам для каждого слоя данных
RAW_DATA_DIR = os.path.join(BASE_DIR, 'data', 'raw')
ODS_DATA_DIR = os.path.join(BASE_DIR, 'data', 'ods')
DDS_DATA_DIR = os.path.join(BASE_DIR, 'data', 'dds')

# --- СПЕЦИФИЧНЫЕ ФУНКЦИИ ДЛЯ ИСТОЧНИКА "ПЕТРОВИЧ" ---

def get_petrovich_raw_db_path():
    """Возвращает путь к SQLite файлу для сырых данных Петровича."""
    return os.path.join(RAW_DATA_DIR, 'petrovich.db')

def init_raw_db_petrovich():
    """
    Инициализирует базу данных для СЫРЫХ данных от парсера "Петрович".
    Создает папку и файл, если они не существуют.
    """
    # Убедимся, что папка /data/raw существует
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    
    db_path = get_petrovich_raw_db_path()
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print(f"Инициализация RAW базы данных для Петровича по пути: {db_path}")
    
    # Создаем таблицу для сырых данных
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS products (
        product_id INTEGER PRIMARY KEY,
        url TEXT,
        title TEXT,
        gold_price REAL,
        retail_price REAL,
        unit TEXT,
        categories TEXT,
        features TEXT,
        raw_html TEXT,
        parsed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    print("Таблица 'products' в RAW слое готова к работе.")
    conn.commit()
    conn.close()

def save_product_to_raw_db_petrovich(product_data):
    """
    Сохраняет одну карточку товара в RAW базу данных Петровича.
    """
    db_path = get_petrovich_raw_db_path()
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Запрос для вставки/замены данных
    cursor.execute('''
    INSERT OR REPLACE INTO products (
        product_id, url, title, gold_price, retail_price, unit, 
        categories, features, raw_html
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        product_data.get('product_id'),
        product_data.get('url'),
        product_data.get('title'),
        product_data.get('gold_price'),
        product_data.get('retail_price'),
        product_data.get('unit'),
        product_data.get('categories'),
        product_data.get('features'),
        product_data.get('raw_html')
    ))
    
    conn.commit()
    conn.close()

# --- ОБЩИЕ ФУНКЦИИ ИЛИ ФУНКЦИИ ДЛЯ ДРУГИХ СЛОЕВ МОЖНО ДОБАВЛЯТЬ НИЖЕ ---
# Например, в будущем здесь могут появиться функции для ODS или DDS, 
# хотя их логичнее вызывать из transform-скриптов.

