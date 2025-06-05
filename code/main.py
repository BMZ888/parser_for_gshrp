# code/main.py

# Импортируем нужные функции из наших модулей
import database
import parser
from tqdm import tqdm
import traceback

def run_parser():
    """Главная функция-оркестратор."""
    # 1. Инициализация
    database.init_db()
    driver = None
    
    try:
        # 2. Запуск браузера
        driver = parser.get_driver()
        if not driver:
            print("Не удалось запустить драйвер. Завершение работы.")
            return

        # 3. Получение ссылок на категории
        category_links = parser.get_category_links(driver)
        if not category_links:
            print("Не удалось получить ссылки на категории. Завершение работы.")
            return
            
        # УБЕРИ/измени для полного парсинга
        category_links = category_links[:1]
        
        total_saved_count = 0
        print(f"\nНачинаю парсинг {len(category_links)} категорий...")

        for category_url in tqdm(category_links, desc="Обработка категорий"):
            page_num = 1
            parsed_in_category_urls = set()
            while True:
                # max_pages_to_parse = 2
                # if page_num > max_pages_to_parse: break

                paginated_url = f"{category_url}?p={page_num}"
                tqdm.write(f"Сканирую: {paginated_url}")
                
                soup = parser.get_
