# src/common/database.py

import sqlite3
import os

# --- КОНФИГУРАЦИЯ ПУТЕЙ (остается без изменений) ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RAW_DATA_DIR = os.path.join(BASE_DIR, 'data', 'raw')
ODS_DATA_DIR = os.path.join(BASE_DIR, 'data', 'ods')
DDS_DATA_DIR = os.path.join(BASE_DIR, 'data', 'dds')

# --- УНИВЕРСАЛЬНАЯ ФУНКЦИЯ (переименована для ясности) ---
def get_db_connection(db_path: str) -> sqlite3.Connection:
    """Возвращает соединение с базой данных."""
    return sqlite3.connect(db_path)

# --- РАБОТА С RAW СЛОЕМ (функции стали универсальными) ---

def get_raw_db_path(source_name: str, is_test: bool = False) -> str:
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    filename = f'{source_name}_test.db' if is_test else f'{source_name}.db'
    return os.path.join(RAW_DATA_DIR, filename)

def init_raw_db(source_name: str, is_test: bool = False):
    """Инициализирует RAW-базу для конкретного источника."""
    db_path = get_raw_db_path(source_name, is_test=is_test)
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    print(f"Инициализация RAW базы для '{source_name}' по пути: {db_path}")
    # Тип product_id изменен на TEXT, так как артикулы могут быть нечисловыми
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS products (
        product_id TEXT PRIMARY KEY,
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
    conn.commit()
    conn.close()

def save_product_to_raw_db(source_name: str, product_data: dict, is_test: bool = False):
    """Сохраняет одну карточку товара в RAW базу конкретного источника."""
    db_path = get_raw_db_path(source_name, is_test=is_test)
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    cursor.execute('''
    INSERT OR REPLACE INTO products (
        product_id, url, title, gold_price, retail_price, unit, 
        categories, features, raw_html
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        product_data.get('product_id'), product_data.get('url'), product_data.get('title'),
        product_data.get('gold_price'), product_data.get('retail_price'),
        product_data.get('unit'), product_data.get('categories'),
        product_data.get('features'), product_data.get('raw_html')
    ))
    conn.commit()
    conn.close()

# --- РАБОТА С ODS СЛОЕМ (НОВЫЙ БЛОК) ---

def get_ods_db_path(source_name: str, is_test: bool = False) -> str:
    os.makedirs(ODS_DATA_DIR, exist_ok=True)
    filename = f'{source_name}_test.db' if is_test else f'{source_name}.db'
    return os.path.join(ODS_DATA_DIR, filename)

def init_ods_db(source_name: str, is_test: bool = False):
    """Инициализирует ODS-базу для конкретного источника."""
    db_path = get_ods_db_path(source_name, is_test=is_test)
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    print(f"\nИнициализация ODS базы для '{source_name}' по пути: {db_path}")
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS ods_products (
        product_id TEXT PRIMARY KEY,
        url TEXT,
        title TEXT,
        gold_price REAL,
        retail_price REAL,
        unit TEXT,
        brand TEXT,
        model TEXT,
        category_l1 TEXT,
        category_l2 TEXT,
        category_l3 TEXT,
        category_l4 TEXT,
        parsed_at TIMESTAMP
    )
    ''')
    conn.commit()
    conn.close()

# --- РАБОТА С DDS СЛОЕМ (НОВЫЙ БЛОК, ОБЩИЙ ФАЙЛ) ---

def get_dds_db_path(is_test: bool = False) -> str:
    os.makedirs(DDS_DATA_DIR, exist_ok=True)
    filename = 'analytics_test.db' if is_test else 'analytics.db'
    return os.path.join(DDS_DATA_DIR, filename)

def init_dds_db(source_name: str, is_test: bool = False):
    """Инициализирует таблицы для КОНКРЕТНОГО источника в общей DWH."""
    db_path = get_dds_db_path(is_test=is_test)
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    print(f"\nИнициализация DDS таблиц для '{source_name}' в общей DWH: {db_path}")
    
    # Таблицы создаются с префиксом источника, чтобы не было конфликтов
    cursor.execute(f'''
    CREATE TABLE IF NOT EXISTS {source_name}_dim_categories (
        category_key INTEGER PRIMARY KEY AUTOINCREMENT,
        category_l1 TEXT, category_l2 TEXT, category_l3 TEXT, category_l4 TEXT,
        UNIQUE(category_l1, category_l2, category_l3, category_l4)
    )''')
    
    cursor.execute(f'''
    CREATE TABLE IF NOT EXISTS {source_name}_dim_brands (
        brand_key INTEGER PRIMARY KEY AUTOINCREMENT,
        brand_name TEXT UNIQUE
    )''')
    
    cursor.execute(f'''
    CREATE TABLE IF NOT EXISTS {source_name}_fact_products (
        product_key INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id TEXT UNIQUE,
        title TEXT,
        category_key INTEGER,
        brand_key INTEGER,
        gold_price REAL,
        retail_price REAL,
        unit TEXT,
        parsed_at TIMESTAMP,
        FOREIGN KEY (category_key) REFERENCES {source_name}_dim_categories (category_key),
        FOREIGN KEY (brand_key) REFERENCES {source_name}_dim_brands (brand_key)
    )''')
    
    conn.commit()
    conn.close()
