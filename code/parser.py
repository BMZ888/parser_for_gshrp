# code/parser.py

import json
import time
import random
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup, NavigableString

BASE_URL = "https://moscow.petrovich.ru"

def get_driver():
    """Настраивает и возвращает экземпляр веб-драйвера с оптимизациями."""
    options = Options()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36")
    prefs = {"profile.managed_default_content_settings.images": 2}
    options.add_experimental_option("prefs", prefs)
    options.binary_location = "/Applications/Yandex.app/Contents/MacOS/Yandex"
    
    # Путь к chromedriver относительно текущего файла (parser.py)
    chromedriver_path = os.path.join(os.path.dirname(__file__), 'chromedriver')
    service = Service(executable_path=chromedriver_path)
    
    driver = webdriver.Chrome(service=service, options=options)
    return driver

def get_page_soup_selenium(driver, url):
    """Загружает страницу с помощью Selenium и возвращает BeautifulSoup объект."""
    driver.get(url)
    time.sleep(random.uniform(3, 5))
    return BeautifulSoup(driver.page_source, 'lxml')

def get_category_links(driver):
    """Собирает все уникальные ссылки на категории товаров."""
    catalog_url = f"{BASE_URL}/catalog/"
    soup = get_page_soup_selenium(driver, catalog_url)
    links = set()
    all_a_tags = soup.find_all('a', href=True)
    for tag in all_a_tags:
        href = tag.get('href')
        if href and href.startswith('/catalog/') and href.count('/') == 3 and href.split('/')[-2].isdigit():
            links.add(BASE_URL + href)
    print(f"Найдено {len(links)} уникальных ТОП-уровневых категорий.")
    return list(links)

# code/parser.py

# ... (импорты в начале файла: json, time, random, selenium, bs4)

# ... (функции get_driver, get_page_soup_selenium, get_category_links)

def parse_product_card(card_soup):
    """
    ФИНАЛЬНАЯ ВЕРСИЯ.
    Парсит ОДНУ карточку товара, извлекая всю доступную информацию.
    """
    # 1. Инициализация словаря с данными
    data = {
        'url': None,
        'title': None,
        'product_id': None,
        'gold_price': None,      # Цена по карте
        'retail_price': None,    # Розничная цена
        'unit': None,            # Единица измерения (шт, м2 и т.д.)
        'categories': [],
        'features': {},
        'raw_html': str(card_soup) # Сохраняем весь HTML карточки для отладки
    }

    # 2. Извлечение основных данных
    
    # Ссылка
    link_tag = card_soup.find('a', attrs={'data-test': 'product-link'})
    if link_tag and link_tag.has_attr('href'):
        data['url'] = BASE_URL + link_tag['href']
    
    # Название
    title_tag = card_soup.find('span', attrs={'data-test': 'product-title'})
    if title_tag:
        data['title'] = title_tag.text.strip()
    
    # ID товара (артикул)
    code_tag = card_soup.find('p', attrs={'data-test': 'product-code'})
    if code_tag:
        try:
            data['product_id'] = int(code_tag.text.strip())
        except (ValueError, IndexError):
            pass # Если не удалось, product_id останется None

    # 3. Извлечение цен и единицы измерения
    
    # Цена по "золотой" карте
    gold_price_tag = card_soup.find('p', attrs={'data-test': 'product-gold-price'})
    if gold_price_tag:
        # get_text() соединеняет текст из всех вложенных тегов
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
        data['unit'] = active_unit_tag.get_text(strip=True)

    # 4. Извлечение категорий и характеристик

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
    
    # 5. Сериализация сложных полей в JSON для сохранения в БД
    data['features'] = json.dumps(data['features'], ensure_ascii=False)
    data['categories'] = json.dumps(data['categories'], ensure_ascii=False)

    return data

