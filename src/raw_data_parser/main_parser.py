# code/main.py

# Импортируем наши модули
import database
import parser

# Импортируем служебные библиотеки
from tqdm import tqdm
import traceback
import os
import time

# --- Конфигурация для восстановления прогресса ---
# Определяем корневую папку проекта
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Путь к файлу, где будем хранить прогресс (журнал выполненных задач)
PROGRESS_FILE = os.path.join(BASE_DIR, 'data', 'completed_categories.txt')


def save_completed_category(category_url):
    """Дописывает URL успешно обработанной категории в файл прогресса."""
    # Убедимся, что папка /data существует
    os.makedirs(os.path.dirname(PROGRESS_FILE), exist_ok=True)
    # Используем режим 'a' (append) для добавления в конец файла
    with open(PROGRESS_FILE, 'a', encoding='utf-8') as f:
        f.write(category_url + '\n')

def load_completed_categories():
    """
    Загружает МНОЖЕСТВО всех ранее обработанных категорий из файла.
    Использование множества (set) автоматически решает проблему дубликатов и
    ускоряет проверку.
    """
    if not os.path.exists(PROGRESS_FILE):
        return set()
    
    with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
        # Считываем все строки, убираем пробелы/переносы и создаем множество
        completed_urls = {line.strip() for line in f if line.strip()}
    return completed_urls

def run_petrovich_parser():
    """
    Главная функция-оркестратор для парсинга сайта 'Петрович'
    с надежной логикой восстановления после сбоев.
    """
    # 1. Инициализация RAW базы данных
    database.init_raw_db_petrovich()
    
    # 2. Получаем ПОЛНЫЙ список категорий ОДИН РАЗ
    print("Получаю полный список категорий для парсинга...")
    driver = None
    all_category_links_list = []
    try:
        driver = parser.get_driver()
        if driver:
            # Используем множество для автоматической дедупликации с самого начала
            all_category_links_list = parser.get_category_links(driver)
    except Exception as e:
        print(f"Критическая ошибка при получении списка категорий: {e}")
    finally:
        if driver:
            driver.quit()
            
    if not all_category_links_list:
        print("Не удалось получить список категорий. Завершение работы.")
        return

    # Преобразуем в множество для эффективных операций
    all_categories_set = set(all_category_links_list)
    print(f"Найдено {len(all_categories_set)} уникальных категорий на сайте.")

    # 3. Определяем, какие категории уже обработаны
    completed_categories = load_completed_categories()
    if completed_categories:
        print(f"Найдено {len(completed_categories)} уже обработанных категорий в файле прогресса.")

    # 4. Вычисляем, какие категории остались для парсинга
    # Используем разность множеств - это быстро и надежно
    categories_to_parse = all_categories_set - completed_categories
    
    if not categories_to_parse:
        print("Отлично! Все категории уже были успешно обработаны!")
        # Опционально: можно тут удалить файл прогресса, если нужно начать с нуля в следующий раз
        # if os.path.exists(PROGRESS_FILE):
        #     os.remove(PROGRESS_FILE)
        return

    print(f"Осталось обработать {len(categories_to_parse)} из {len(all_categories_set)} категорий.")
    time.sleep(3) # Пауза, чтобы можно было прочитать сообщение

    # 5. Главный цикл парсинга с возможностью перезапуска
    driver = None
    try:
        print("\n--- Запуск сессии парсинга ---")
        driver = parser.get_driver()
        
        total_saved_count = 0
        
        # tqdm будет работать с множеством, порядок не гарантирован, но это и не важно
        for category_url in tqdm(categories_to_parse, desc="Обработка категорий"):
            page_num = 1
            # Эта переменная теперь менее важна, т.к. мы не перезапускаем категорию,
            # но оставим ее для защиты от зацикливания на одной странице
            parsed_in_category_urls = set()
            while True:
                paginated_url = f"{category_url}?p={page_num}"
                
                try:
                    soup = parser.get_page_soup_selenium(driver, paginated_url)
                except Exception as e:
                    tqdm.write(f"\nОшибка при загрузке страницы {paginated_url}: {e}")
                    tqdm.write("Потеряна сессия с браузером. Парсер будет перезапущен.")
                    raise e # Выбрасываем исключение наверх для корректной остановки

                if not soup: break
                
                product_containers = soup.find_all('div', attrs={'data-test': 'product-card-catalog-wide'})
                if not product_containers: break
                
                # Проверка на зацикливание (если сайт отдает одну и ту же страницу)
                first_product_link = product_containers[0].find('a', attrs={'data-test': 'product-link'})
                current_first_url = first_product_link['href'] if first_product_link else None
                if current_first_url and current_first_url in parsed_in_category_urls:
                    tqdm.write(f"Обнаружено зацикливание в категории {category_url}. Переход к следующей.")
                    break
                if current_first_url:
                    parsed_in_category_urls.add(current_first_url)
                
                for container in product_containers:
                    product_data = parser.parse_product_card(container)
                    if product_data.get('product_id'):
                        database.save_product_to_raw_db_petrovich(product_data)
                        total_saved_count += 1
                
                page_num += 1

            # После УСПЕШНОЙ обработки всех страниц категории - записываем ее в журнал
            save_completed_category(category_url)
            tqdm.write(f'Категория {category_url} успешно обработана и записана в журнал.')
        
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


if __name__ == "__main__":
    run_petrovich_parser()
