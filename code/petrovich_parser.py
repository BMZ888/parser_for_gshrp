# petrovich_parser.py

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup, NavigableString
import time
import random
from pprint import pprint
import json 

# --- Константы ---
BASE_URL = "https://moscow.petrovich.ru"

# --- Настройка Selenium для Яндекс.Браузера на macOS ---
def get_driver():
    """Настраивает и возвращает экземпляр веб-драйвера с оптимизациями."""
    options = Options()
    
    # Маскировка под обычного пользователя
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36")
    
    # --- ОПТИМИЗАЦИЯ ---
    # Отключаем загрузку изображений для ускорения
    prefs = {"profile.managed_default_content_settings.images": 2}
    options.add_experimental_option("prefs", prefs)
    
    # options.add_argument("--headless") # В режиме headless сайт может вести себя иначе, пока лучше без него
    
    options.binary_location = "/Applications/Yandex.app/Contents/MacOS/Yandex"
    service = Service(executable_path='./chromedriver')
    
    try:
        driver = webdriver.Chrome(service=service, options=options)
        return driver
    except Exception as e:
        print(f"Ошибка при инициализации драйвера: {e}")
        return None

def get_page_soup_selenium(driver, url):
    """Загружает страницу с помощью Selenium и возвращает BeautifulSoup объект."""
    try:
        driver.get(url)
        print(f"Загружаю страницу: {url}. Жду 5-8 секунд для прогрузки...")
        time.sleep(random.uniform(5, 8))
        html = driver.page_source
        return BeautifulSoup(html, 'lxml')
    except Exception as e:
        print(f"Ошибка при загрузке {url} через Selenium: {e}")
        return None

def get_category_links(driver):
    """Собирает все уникальные ссылки на категории товаров."""
    catalog_url = f"{BASE_URL}/catalog/"
    print(f"\nНачинаю сбор ссылок на категории с: {catalog_url}")
    soup = get_page_soup_selenium(driver, catalog_url)
    if not soup:
        return []
    links = set()
    all_a_tags = soup.find_all('a', href=True)
    for tag in all_a_tags:
        href = tag.get('href')
        if href and href.startswith('/catalog/') and href.count('/') == 3 and href.split('/')[-2].isdigit():
            links.add(BASE_URL + href)
    print(f"Найдено {len(links)} уникальных ссылок на ТОП-уровневые категории.")
    return list(links)
    
# --- НОВАЯ, МОЩНАЯ ФУНКЦИЯ ПАРСИНГА КАРТОЧКИ ТОВАРА ---

def parse_product_card(card_soup):
    """
    ФИНАЛЬНАЯ ВЕРСИЯ.
    Парсит ОДНУ карточку товара, извлекая всю доступную информацию.
    """
    data = {
        'url': None,
        'title': None,
        'product_id': None,
        'gold_price': None,      # Цена по карте
        'retail_price': None,    # Розничная цена
        'unit': None,            # Единица измерения (шт, м2 и т.д.)
        'categories': [],
        'features': {},
        'raw_html': str(card_soup) # Сохраняем весь HTML карточки на всякий случай
    }

    # -- Извлечение основных данных --

    # Ссылка, ID, Название
    link_tag = card_soup.find('a', attrs={'data-test': 'product-link'})
    if link_tag and link_tag.has_attr('href'):
        data['url'] = BASE_URL + link_tag['href']
    
    title_tag = card_soup.find('span', attrs={'data-test': 'product-title'})
    if title_tag:
        data['title'] = title_tag.text.strip()
    
    code_tag = card_soup.find('p', attrs={'data-test': 'product-code'})
    if code_tag:
        try:
            data['product_id'] = int(code_tag.text.strip())
        except (ValueError, IndexError):
            pass

    # -- Извлечение цен и единицы измерения --
    
    # Цена по "золотой" карте
    gold_price_tag = card_soup.find('p', attrs={'data-test': 'product-gold-price'})
    if gold_price_tag:
        price_str = gold_price_tag.get_text(strip=True).replace('₽', '').replace('\u2009', '').replace(',', '.')
        try:
            data['gold_price'] = float(price_str)
        except (ValueError, TypeError):
            pass

    # Розничная цена (обычная)
    retail_price_tag = card_soup.find('p', attrs={'data-test': 'product-retail-price'})
    if retail_price_tag:
        price_str = retail_price_tag.get_text(strip=True).replace('₽', '').replace('\u2009', '').replace(',', '.')
        try:
            data['retail_price'] = float(price_str)
        except (ValueError, TypeError):
            pass
            
    # Единица измерения (очень важный параметр для цены)
    # Находим активный таб с единицей измерения
    active_unit_tag = card_soup.find('div', class_=lambda c: c and 'tab-active' in c and 'price-switcher-tab' in c)
    if active_unit_tag:
        # Убираем лишние символы вроде "3" из "м3"
        data['unit'] = active_unit_tag.get_text(strip=True)

    # -- Извлечение категорий и характеристик --

    # "Хлебные крошки" (категории)
    breadcrumbs_div = card_soup.find('div', attrs={'data-test': 'product-breadcrumbs'})
    if breadcrumbs_div:
        data['categories'] = [cat.get_text(strip=True) for cat in breadcrumbs_div.find_all('a')]

    # Характеристики
    description_p = card_soup.find('p', attrs={'data-test': 'product-description'})
    if description_p:
        # Преобразуем <br> в специальный разделитель, чтобы потом разбить строку
        for br in description_p.find_all('br'):
            br.replace_with('|||')
        
        # Получаем текст и разбиваем на отдельные характеристики
        full_text = description_p.get_text(strip=True)
        features_list = [item.strip() for item in full_text.split('|||') if item.strip()]
        
        for feature_item in features_list:
            if ':' in feature_item:
                key, value = feature_item.split(':', 1)
                data['features'][key.strip()] = value.strip()
    
    # Чтобы характеристики хранились в виде JSON-строки в БД, можно сделать так:
    data['features'] = json.dumps(data['features'], ensure_ascii=False)
    # И категории тоже
    data['categories'] = json.dumps(data['categories'], ensure_ascii=False)

    return data




# def get_all_products_data(driver):
#     """
#     Главная функция. Проходит по категориям и собирает данные о товарах,
#     корректно обрабатывая окончание пагинации.
#     """
#     category_links = get_category_links(driver)
#     if not category_links:
#         return []
    
#     # Для теста возьмем только одну категорию
#     category_links = category_links[:1]
#     print(f"\nНачинаю парсинг {len(category_links)} категорий...")
    
#     all_products = []

#     for category_url in category_links:
#         page_num = 1
#         max_pages = 100 # Оставляем как страховку
        
#         # --- КЛЮЧЕВОЕ ИЗМЕНЕНИЕ: Отслеживаем已собранные URLы ---
#         parsed_in_category_urls = set()
        
#         print(f"\n--- Парсинг категории: {category_url} ---")

#         while page_num <= max_pages:
#             paginated_url = f"{category_url}?p={page_num}"
#             soup = get_page_soup_selenium(driver, paginated_url)
#             if not soup:
#                 break

#             product_containers = soup.find_all('div', attrs={'data-test': 'product-card-catalog-wide'})
#             if not product_containers:
#                 print(f"  Страница {page_num}. Товары не найдены. Завершаю для этой категории.")
#                 break
            
#             # --- НОВАЯ ПРОВЕРКА НА ДУБЛИКАТЫ ---
#             # Получаем URL первого товара на странице
#             first_product_link_tag = product_containers[0].find('a', attrs={'data-test': 'product-link'})
#             if first_product_link_tag and first_product_link_tag.has_attr('href'):
#                 first_product_url = first_product_link_tag['href']
#                 # Если URL первого товара уже был в нашем списке, значит мы зациклились
#                 if first_product_url in parsed_in_category_urls:
#                     print(f"  Страница {page_num}. Обнаружен дубликат. Завершаю для этой категории.")
#                     break
#             # ------------------------------------

#             print(f"  Страница {page_num}. Найдено {len(product_containers)} товаров. Парсинг...")
#             for container in product_containers:
#                 product_data = parse_product_card(container)
#                 # Добавляем URL в наш сет для отслеживания
#                 if product_data['url']:
#                     # Добавляем относительный URL, так как именно он уникален внутри сайта
#                     relative_url = product_data['url'].replace(BASE_URL, '')
#                     parsed_in_category_urls.add(relative_url)
                
#                 all_products.append(product_data)
            
#             page_num += 1

#     return all_products

# for TESTING 
# Замени только эту функцию в своем коде
def get_all_products_data(driver):
    """
    Главная функция. Проходит по категориям и собирает данные о товарах.
    Очищает cookies перед каждой новой категорией.
    """
    category_links = get_category_links(driver)
    if not category_links:
        return []
    
    # УБЕРИ/измени это ограничение для полного парсинга
    category_links = category_links[:3] 
    
    print(f"\nНачинаю парсинг {len(category_links)} категорий...")
    
    all_products = []

    # Используем tqdm для красивого прогресс-бара по категориям
    from tqdm import tqdm
    for category_url in tqdm(category_links, desc="Парсинг категорий"):
        
        # --- ОЧИСТКА COOKIES ---
        # print(f"\nОчищаю cookies перед заходом в категорию: {category_url}")
        # driver.delete_all_cookies() # Может потребовать повторного прохождения JS-защиты

        # --- Парсинг страниц в категории ---
        page_num = 1
        max_pages_to_parse = 2 # Ограничение для теста
        
        parsed_in_category_urls = set()
        
        while page_num <= max_pages_to_parse:
            paginated_url = f"{category_url}?p={page_num}"
            soup = get_page_soup_selenium(driver, paginated_url)
            if not soup:
                break

            product_containers = soup.find_all('div', attrs={'data-test': 'product-card-catalog-wide'})
            if not product_containers:
                break
            
            first_product_link_tag = product_containers[0].find('a', attrs={'data-test': 'product-link'})
            if first_product_link_tag and first_product_link_tag.has_attr('href'):
                if first_product_link_tag['href'] in parsed_in_category_urls:
                    break

            # Убрал подробный вывод для чистоты лога, когда работает tqdm
            # print(f"  Страница {page_num}. Найдено {len(product_containers)} товаров. Парсинг...")
            for container in product_containers:
                product_data = parse_product_card(container)
                if product_data.get('url'):
                    relative_url = product_data['url'].replace(BASE_URL, '')
                    parsed_in_category_urls.add(relative_url)
                all_products.append(product_data)
            
            page_num += 1

    return all_products



# --- Основной блок выполнения скрипта ---
if __name__ == "__main__":
    driver = None
    try:
        print("Запуск браузера...")
        driver = get_driver()
        
        if driver:
            all_products_data = get_all_products_data(driver)
            
            if all_products_data:
                print(f"\n\n--- Сбор данных завершен! ---")
                print(f"Всего собрано {len(all_products_data)} товаров.")
                print("\nПример данных по первому товару:")
                pprint(all_products_data[0], sort_dicts=False)
                print("\nПример данных по последнему товару:")
                pprint(all_products_data[-1], sort_dicts=False)
            else:
                print("Не удалось собрать данные о товарах.")

    except Exception as e:
        import traceback
        print(f"Произошла непредвиденная ошибка в основном блоке: {e}")
        traceback.print_exc()

    finally:
        if driver:
            driver.quit()
            print("\nБраузер успешно закрыт.")

# --- ОТЛАДОЧНЫЙ БЛОК ---
# if __name__ == "__main__":
#     driver = None
#     try:
#         print("Запуск браузера в режиме отладки...")
#         driver = get_driver()
        
#         if driver:
#             # Возьмем категорию, где точно есть товары с ценой
#             test_category_url = "https://moscow.petrovich.ru/catalog/12101/" # Гипсокартон
#             soup = get_page_soup_selenium(driver, test_category_url)

#             if soup:
#                 # Находим первую карточку товара на странице
#                 first_card = soup.find('div', attrs={'data-test': 'product-card-catalog-wide'})
                
#                 if first_card:
#                     print("Нашел первую карточку товара. Сохраняю ее HTML в файл...")
#                     # Используем .prettify() для красивого форматирования HTML
#                     card_html = first_card.prettify()
                    
#                     with open("debug_product_card.html", "w", encoding="utf-8") as f:
#                         f.write(card_html)
                    
#                     print("\nHTML-код карточки сохранен в файл 'debug_product_card.html'.")
#                     print("Пожалуйста, открой этот файл и посмотри, где находится цена.")
#                     print("Можно прислать его содержимое мне.")

#                 else:
#                     print("Не удалось найти ни одной карточки товара на странице.")
#             else:
#                 print("Не удалось получить soup страницы.")

#     except Exception as e:
#         import traceback
#         print(f"Произошла непредвиденная ошибка в основном блоке: {e}")
#         traceback.print_exc()

#     finally:
#         if driver:
#             driver.quit()
#             print("\nБраузер успешно закрыт.")