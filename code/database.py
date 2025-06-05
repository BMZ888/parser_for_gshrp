# code/database.py

import sqlite3
import os

# Путь к папке data относительно папки code
DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
DB_NAME = os.path.join(DATA_DIR, "petrovich_data.db")

def init_db():
    """Инициализирует базу данных и создает таблицу в папке /data."""
    # Убедимся, что папка /data существует
    os.makedirs(DATA_DIR, exist_ok=True)
    
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    print(f"База данных находится по пути: {DB_NAME}")
    
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
    print("Таблица 'products' готова к работе.")
    conn.commit()
    conn.close()

def save_product_to_db(product_data):
    """Сохраняет один товар в базу данных."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
    INSERT OR REPLACE INTO products (
        product_id, url, title, gold_price, retail_price, unit, 
        categories, features, raw_html
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        product_data.get('product_id'), product_data.get('url'), product_data.get('title'),
        product_data.get('gold_price'), product_data.get('retail_price'), product_data.get('unit'),
        product_data.get('categories'), product_data.get('features'), product_data.get('raw_html')
    ))
    conn.commit()
    conn.close()
