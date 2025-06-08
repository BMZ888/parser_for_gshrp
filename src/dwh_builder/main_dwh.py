# src/dwh_builder/main_dwh.py
import sqlite3
from src.common import database
from src.dwh_builder import transformer
from tqdm import tqdm

def _get_or_create_dimension_key(cursor, table_name_with_prefix: str, key_column: str, value_column: str, value):
    """Вспомогательная универсальная функция для получения ключа измерения (например, бренда)."""
    if value is None:
        return None
    cursor.execute(f'SELECT {key_column} FROM {table_name_with_prefix} WHERE {value_column} = ?', (value,))
    result = cursor.fetchone()
    if result:
        return result[0]
    cursor.execute(f'INSERT INTO {table_name_with_prefix} ({value_column}) VALUES (?)', (value,))
    return cursor.lastrowid

def _get_or_create_category_key(cursor, table_name_with_prefix: str, categories: dict):
    """Вспомогательная функция для получения ключа категории."""
    cat_values = tuple(categories.values())
    # SQL-запрос для поиска существующей категории
    select_sql = f'''
        SELECT category_key FROM {table_name_with_prefix}
        WHERE COALESCE(category_l1, '') = COALESCE(?, '')
          AND COALESCE(category_l2, '') = COALESCE(?, '')
          AND COALESCE(category_l3, '') = COALESCE(?, '')
          AND COALESCE(category_l4, '') = COALESCE(?, '')
    '''
    cursor.execute(select_sql, cat_values)
    result = cursor.fetchone()
    if result:
        return result[0]
    # Если не нашли, вставляем новую
    insert_sql = f'INSERT INTO {table_name_with_prefix} (category_l1, category_l2, category_l3, category_l4) VALUES (?, ?, ?, ?)'
    cursor.execute(insert_sql, cat_values)
    return cursor.lastrowid

def run_dwh_build(source_name: str, is_test: bool = False):
    """
    Главная функция для построения ODS и DDS слоев для конкретного источника.
    """
    print(f"\nЗапуск построения DWH для источника: '{source_name.upper()}'")
    if is_test:
        print("--- РЕЖИМ ТЕСТИРОВАНИЯ: используются тестовые базы данных ---")

    # --- ЭТАП 2.1: Перенос из RAW в ODS ---
    database.init_ods_db(source_name, is_test=is_test)
    raw_conn = database.get_db_connection(database.get_raw_db_path(source_name, is_test=is_test))
    raw_conn.row_factory = sqlite3.Row
    ods_conn = database.get_db_connection(database.get_ods_db_path(source_name, is_test=is_test))
    all_raw_products = raw_conn.cursor().execute("SELECT * FROM products").fetchall()

    if not all_raw_products:
        print(f"В RAW слое для '{source_name}' нет данных для обработки.")
        raw_conn.close()
        ods_conn.close()
        return

    print(f"Начинаю перенос {len(all_raw_products)} записей из RAW в ODS...")
    for row in tqdm(all_raw_products, desc=f"RAW -> ODS для {source_name}"):
        ods_product, _ = transformer.transform_row(row)
        ods_conn.cursor().execute('''
            INSERT OR REPLACE INTO ods_products 
            (product_id, url, title, gold_price, retail_price, unit, brand, model,
             category_l1, category_l2, category_l3, category_l4, parsed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', tuple(ods_product.values()))
    ods_conn.commit()
    print("Перенос в ODS завершен.")
    raw_conn.close()
    ods_conn.close()

    # --- ЭТАП 2.2: Построение DDS из ODS ---
    
    # ИСПРАВЛЕНИЯ БЫЛИ В ЭТОМ БЛОКЕ
    database.init_dds_db(source_name, is_test=is_test)
    ods_conn = database.get_db_connection(database.get_ods_db_path(source_name, is_test=is_test))
    ods_conn.row_factory = sqlite3.Row
    dds_conn = database.get_db_connection(database.get_dds_db_path(is_test=is_test))
    # КОНЕЦ ИСПРАВЛЕНИЙ

    all_ods_products = ods_conn.cursor().execute("SELECT * FROM ods_products").fetchall()
    
    print(f"Начинаю построение витрин DDS из ODS ({len(all_ods_products)} записей)...")
    for row in tqdm(all_ods_products, desc=f"ODS -> DDS для {source_name}"):
        dds_cursor = dds_conn.cursor()
        brand_key = _get_or_create_dimension_key(dds_cursor, f'{source_name}_dim_brands', 'brand_key', 'brand_name', row['brand'])
        category_key = _get_or_create_category_key(dds_cursor, f'{source_name}_dim_categories', {
            'l1': row['category_l1'], 'l2': row['category_l2'],
            'l3': row['category_l3'], 'l4': row['category_l4']
        })
        
        fact_values = (
            row['product_id'], row['title'], category_key, brand_key,
            row['gold_price'], row['retail_price'], row['unit'], row['parsed_at']
        )
        dds_cursor.execute(f'''
            INSERT OR REPLACE INTO {source_name}_fact_products
                (product_id, title, category_key, brand_key, gold_price, retail_price, unit, parsed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', fact_values)

    dds_conn.commit()
    print("Построение DDS завершено.")
    ods_conn.close()
    dds_conn.close()

