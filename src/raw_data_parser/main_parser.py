#src/raw_data_parser/main_parser.py

# Импортируем наши модули
from src.common import database
from src.raw_data_parser import parser

# Импортируем служебные библиотеки
from tqdm import tqdm
import traceback
import os
import time

# --- Функции для управления прогрессом парсинга ---

def get_progress_file_path(is_test: bool = False) -> str:
    """Возвращает путь к файлу прогресса (рабочему или тестовому)."""
    # Определяем базовую директорию проекта
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    filename = 'completed_categories_test.txt' if is_test else 'completed_categories.txt'
    return os.path.join(base_dir, 'data', filename)

def save_completed_category(category_url: str, is_test: bool = False):
    """Дописывает URL успешно обработанной категории в правильный файл прогресса."""
    progress_file = get_progress_file_path(is_test)
    os.makedirs(os.path.dirname(progress_file), exist_ok=True)
    with open(progress_file, 'a', encoding='utf-8') as f:
        f.write(category_url + '\n')

def load_completed_categories(is_test: bool = False) -> set:
    """Загружает МНОЖЕСТВО ранее обработанных категорий из правильного файла."""
    progress_file = get_progress_file_path(is_test)
    if not os.path.exists(progress_file):
        return set()
    with open(progress_file, 'r', encoding='utf-8') as f:
        return {line.strip() for line in f if line.strip()}

# --- Основная функция-оркестратор парсера ---

def run_petrovich_parser(is_test: bool = False):
    """
    Главная функция для парсинга сайта 'Петрович' с поддержкой тестового режима.
    """
    SOURCE_NAME = 'petrovich'
    
    # 1. Инициализация RAW базы данных с учетом тестового режима
    database.init_raw_db(SOURCE_NAME, is_test=is_test)
    
    # 2. Получение списка категорий
    print("Получаю полный список категорий для парсинга...")
    driver = None
    all_category_links = []
    try:
        driver = parser.get_driver()
        if driver:
            all_category_links = parser.get_category_links(driver)
    except Exception as e:
        print(f"Критическая ошибка при получении списка категорий: {e}")
    finally:
        if driver:
            driver.quit()
            
    if not all_category_links:
        print("Не удалось получить список категорий. Завершение работы.")
        return
        
    # --- ОГРАНИЧЕНИЕ ДЛЯ ТЕСТОВОГО РЕЖИМА ---
    if is_test:
        print("\n============== РЕЖИМ ТЕСТИРОВАНИЯ: будет обработана только 1 категория ==============\n")
        all_category_links = all_category_links[:1] # Берем только первую категорию
    
    categories_to_parse_set = set(all_category_links)

    # 3. Определение прогресса с учетом тестового режима
    completed_categories = load_completed_categories(is_test=is_test)
    categories_to_parse = list(categories_to_parse_set - completed_categories)
    
    if not categories_to_parse:
        print("Отлично! Все категории уже были успешно обработаны!")
        return

    print(f"Осталось обработать {len(categories_to_parse)} из {len(categories_to_parse_set)} категорий.")
    time.sleep(2)

    # 4. Главный цикл парсинга
    driver = None
    try:
        print("\n--- Запуск сессии парсинга ---")
        driver = parser.get_driver()
        total_saved_count = 0
        
        for category_url in tqdm(categories_to_parse, desc="Обработка категорий"):
            page_num = 1
            parsed_in_category_urls = set()
            while True:
                paginated_url = f"{category_url}?p={page_num}"
                try:
                    soup = parser.get_page_soup_selenium(driver, paginated_url)
                except Exception as e:
                    tqdm.write(f"\nОшибка при загрузке страницы {paginated_url}: {e}")
                    raise e

                if not soup: break
                
                product_containers = soup.find_all('div', attrs={'data-test': 'product-card-catalog-wide'})
                if not product_containers: break
                
                first_product_link = product_containers[0].find('a', attrs={'data-test': 'product-link'})
                current_first_url = first_product_link['href'] if first_product_link else None
                if current_first_url and current_first_url in parsed_in_category_urls:
                    break
                if current_first_url:
                    parsed_in_category_urls.add(current_first_url)
                
                for container in product_containers:
                    product_data = parser.parse_product_card(container)
                    if product_data.get('product_id'):
                        # Сохраняем в правильную базу с учетом тестового режима
                        database.save_product_to_raw_db(SOURCE_NAME, product_data, is_test=is_test)
                        total_saved_count += 1
                
                page_num += 1

            # Сохраняем прогресс в правильный файл
            save_completed_category(category_url, is_test=is_test)
        
        print(f"\n\n--- Сбор данных с сайта 'Петрович' успешно завершен! ---")
        print(f"Всего сохранено/обновлено в RAW базу за эту сессию: {total_saved_count} товаров.")

    except Exception as e:
        print(f"\nПроизошла непредвиденная ошибка в процессе парсинга: {e}")
        traceback.print_exc()
        print("\nПопробуйте запустить скрипт снова, он должен продолжить с места остановки.")
    finally:
        if driver:
            driver.quit()
            print("\nБраузер успешно закрыт.")
