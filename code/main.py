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
                
                soup = parser.get_page_soup_selenium(driver, paginated_url)
                if not soup: break
                
                product_containers = soup.find_all('div', attrs={'data-test': 'product-card-catalog-wide'})
                if not product_containers: break
                
                first_product_link_tag = product_containers[0].find('a', attrs={'data-test': 'product-link'})
                if first_product_link_tag and first_product_link_tag.has_attr('href'):
                    if first_product_link_tag['href'] in parsed_in_category_urls:
                        tqdm.write("Обнаружен дубликат, переход к следующей категории.")
                        break
                
                for container in product_containers:
                    product_data = parser.parse_product_card(container)
                    if product_data.get('product_id'):
                        database.save_product_to_db(product_data)
                        total_saved_count += 1
                        relative_url = product_data['url'].replace(parser.BASE_URL, '')
                        parsed_in_category_urls.add(relative_url)
                page_num += 1
        
        print(f"\n\n--- Сбор данных завершен! ---")
        print(f"Всего сохранено/обновлено в БД: {total_saved_count} товаров.")

    except Exception as e:
        print(f"\nПроизошла непредвиденная ошибка: {e}")
        traceback.print_exc()
    finally:
        if driver:
            driver.quit()
            print("\nБраузер успешно закрыт.")

if __name__ == "__main__":
    run_parser()
