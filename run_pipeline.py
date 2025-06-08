# run_parser.py (или run_pipeline.py)

import sys
import os

# Добавляем папку src в системный путь
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

# Импортируем наши модули-оркестраторы
from raw_data_parser import main_parser
from dwh_builder import main_dwh

def run_pipeline_for_source(source_name: str, is_test: bool = False):
    """
    Запускает полный конвейер (ETL) для одного источника
    с поддержкой тестового режима.
    """
    if is_test:
        print("="*60)
        print("==============      ЗАПУСК В ТЕСТОВОМ РЕЖИМЕ      ==============")
        print("="*60)
    
    print(f"\n--- ЗАПУСК КОНВЕЙЕРА ДАННЫХ ДЛЯ ИСТОЧНИКА: '{source_name.upper()}' ---")
    
    # --- Этап 1: Сбор сырых данных ---
    
    # В будущем здесь будет if/else для вызова парсера для 'leroymerlin' и т.д.
    if source_name == "petrovich":
        print(f"\n[ЭТАП 1/2] Запуск парсера для '{source_name}'...")
        try:
            # Передаем флаг is_test в функцию парсера
            main_parser.run_petrovich_parser(is_test=is_test)
            print(f"[ЭТАП 1/2] Сбор сырых данных для '{source_name}' успешно завершен.")
        except Exception as e:
            print(f"[ЭТАП 1/2] КРИТИЧЕСКАЯ ОШИБКА ПАРСЕРА: {e}")
            # Если парсер упал, нет смысла идти дальше
            return
    else:
        print(f"Парсер для источника '{source_name}' не реализован.")
        return

    # --- Этап 2: Трансформация данных и построение DWH ---
    print(f"\n[ЭТАП 2/2] Запуск трансформации данных и построение DWH для '{source_name}'...")
    try:
        # Передаем оба параметра: имя источника и флаг теста
        main_dwh.run_dwh_build(source_name=source_name, is_test=is_test)
    except Exception as e:
        print(f"[ЭТАП 2/2] КРИТИЧЕСКАЯ ОШИБКА ТРАНСФОРМАЦИИ: {e}")

    print(f"\n--- РАБОТА КОНВЕЙЕРА ДЛЯ '{source_name.upper()}' ЗАВЕРШЕНА ---")

if __name__ == "__main__":
    # === ГЛАВНЫЙ ПЕРЕКЛЮЧАТЕЛЬ РЕЖИМОВ ===
    # Чтобы запустить полный, "боевой" прогон, установите TEST_MODE = False
    # Чтобы запустить быстрый тест на 1 категории, установите TEST_MODE = True
    TEST_MODE = False 

    # === КОНФИГУРАЦИЯ ИСТОЧНИКА ===
    # Здесь мы определяем, какой источник обрабатывать.
    SOURCE_TO_PROCESS = "petrovich"
    
    run_pipeline_for_source(SOURCE_TO_PROCESS, is_test=TEST_MODE)
