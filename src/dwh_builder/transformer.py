# src/dwh_builder/transformer.py

import json

def transform_row(raw_row):
    """
    Трансформирует одну сырую строку из RAW в структурированный 
    словарь для ODS и DDS слоев.
    """
    
    # --- Извлекаем и преобразуем поля ---
    
    # 1. Распарсим JSON поля
    try:
        categories_list = json.loads(raw_row['categories'])
    except (json.JSONDecodeError, TypeError):
        categories_list = []

    try:
        features_dict = json.loads(raw_row['features'])
    except (json.JSONDecodeError, TypeError):
        features_dict = {}

    # 2. Очистим и структурируем категории
    cat_levels = {
        'category_l1': categories_list[0] if len(categories_list) > 0 else None,
        'category_l2': categories_list[1] if len(categories_list) > 1 else None,
        'category_l3': categories_list[2] if len(categories_list) > 2 else None,
        'category_l4': categories_list[3] if len(categories_list) > 3 else None,
    }

    # 3. Извлечем и ОЧИСТИМ ключевые характеристики
    brand = features_dict.get('Бренд')
    model = features_dict.get('Модель')
    
    # ## >> ИСПРАВЛЕННАЯ ЛОГИКА << ##
    # Считываем значение из raw_row...
    unit = raw_row['unit'] 
    # ...и если оно пустое, заменяем его на 'шт'
    if not unit:
        unit = 'шт'
    
    # 4. Собираем чистый ODS-объект
    ods_product = {
        'product_id': raw_row['product_id'],
        'url': raw_row['url'],
        'title': raw_row['title'],
        'gold_price': raw_row['gold_price'],
        'retail_price': raw_row['retail_price'],
        'unit': unit, # <-- ИСПОЛЬЗУЕМ ОЧИЩЕННУЮ ПЕРЕМЕННУЮ unit
        'brand': brand,
        'model': model,
        'parsed_at': raw_row['parsed_at'],
        **cat_levels
    }

    # 5. Собираем объекты для DDS слоя
    # Этот блок на самом деле не используется в нашей новой логике main_dwh.py,
    # но оставим его корректным на всякий случай.
    dds_data = {
        'fact_product': {
            'product_id': raw_row['product_id'],
            'title': raw_row['title'],
            'gold_price': raw_row['gold_price'],
            'retail_price': raw_row['retail_price'],
            'unit': unit, # <-- И ЗДЕСЬ ТОЖЕ ИСПОЛЬЗУЕМ ОЧИЩЕННУЮ ПЕРЕМЕННУЮ
            'parsed_at': raw_row['parsed_at'],
        },
        'dim_brand': { 'brand_name': brand },
        'dim_category': cat_levels
    }
    
    return ods_product, dds_data

