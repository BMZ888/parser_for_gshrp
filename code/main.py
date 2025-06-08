# code/main.py

# Импортируем наши модули
import database
import parser

# Импортируем служебные библиотеки
from tqdm import tqdm
import traceback

def run_petrovich_parser():
    """
    Главная функция-оркестратор для парсинга сайта 'Петрович'.
    """
    # 1. Инициализация RAW базы данных для Петровича
    database.init_raw_db_petrovich()
    driver = None
    
    try:
        # 2. Запуск браузера
        print("Запуск браузера для парсинга Петровича...")
        driver = parser.get_driver()
        if not driver:
            print("Не удалось запустить драйвер. Завершение работы.")
            return

        # 3. Получение ссылок на категории
        category_links = parser.get_category_links(driver)
        if not category_links:
            print("Не удалось получить ссылки на категории. Завершение работы.")
            return
            
        # --- УБЕРИ/измени для полного парсинга ---
        # category_links = category_links[:1] 
        
        total_saved_count = 0
        print(f"\nНачинаю парсинг {len(category_links)} категорий...")

        for category_url in tqdm(category_links, desc="Обработка категорий"):
            page_num = 1
            parsed_in_category_urls = set()
            while True:
                # --- УБЕРИ/измени для полного парсинга страниц ---
                # max_pages_to_parse = 2
                # if page_num > max_pages_to_parse: break

                paginated_url = f"{category_url}?p={page_num}"
                
                try:
                    soup = parser.get_page_soup_selenium(driver, paginated_url)
                except Exception as e:
                    tqdm.write(f"Ошибка при загрузке страницы {paginated_url}: {e}")
                    break # Прерываем парсинг этой категории при ошибке
                
                if not soup: break
                
                product_containers = soup.find_all('div', attrs={'data-test': 'product-card-catalog-wide'})
                if not product_containers:
                    # Это штатное завершение пагинации
                    break
                
                # Проверка на зацикливание
                first_product_link_tag = product_containers[0].find('a', attrs={'data-test': 'product-link'})
                if first_product_link_tag and first_product_link_tag.has_attr('href'):
                    if first_product_link_tag['href'] in parsed_in_category_urls:
                        tqdm.write("Обнаружен дубликат, переход к следующей категории.")
                        break
                
                for container in product_containers:
                    product_data = parser.parse_product_card(container)
                    if product_data.get('product_id'):
                        # 4. Вызов обновленной функции сохранения
                        database.save_product_to_raw_db_petrovich(product_data)
                        total_saved_count += 1
                        relative_url = product_data['url'].replace(parser.BASE_URL, '')
                        parsed_in_category_urls.add(relative_url)
                page_num += 1
        
        print(f"\n\n--- Сбор данных с сайта 'Петрович' завершен! ---")
        print(f"Всего сохранено/обновлено в RAW базу: {total_saved_count} товаров.")

    except Exception as e:
        print(f"\nПроизошла непредвиденная ошибка в процессе парсинга: {e}")
        traceback.print_exc()
    finally:
        if driver:
            driver.quit()
            print("\nБраузер успешно закрыт.")


if __name__ == "__main__":
    run_petrovich_parser()
