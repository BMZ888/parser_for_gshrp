import os
import json
import time
import random
from selenium import webdriver
from selenium.webdriver.chrome.service import Service  # <-- Используем именно Service
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup, NavigableString
from dotenv import load_dotenv
from webdriver_manager.chrome import ChromeDriverManager


BASE_URL = "https://moscow.petrovich.ru"
def get_driver():
    """
    Настраивает и возвращает экземпляр веб-драйвера для Яндекс.Браузера,
    используя правильную версию chromedriver.
    """
    load_dotenv()
    options = Options()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36")
    
    # --- БЛОК РАБОТЫ С ЯНДЕКС.БРАУЗЕРОМ ---
    yandex_path = os.getenv('YANDEX_BROWSER_PATH')
    if yandex_path and os.path.exists(yandex_path):
        print("Найден путь к Яндекс.Браузеру. Использую его.")
        options.binary_location = yandex_path
    else:
        print("Путь к Яндекс.Браузеру в .env не найден или неверен. Запуск может не удаться.")
        # Можно завершить работу, если Яндекс.Браузер обязателен
        # return None
    
    # --- УКАЗЫВАЕМ ВЕРСИЮ ДРАЙВЕРА ---
    driver_version = os.getenv('CHROME_DRIVER_VERSION')
    if not driver_version:
        print("Версия драйвера CHROME_DRIVER_VERSION не найдена в .env. Менеджер попробует угадать.")

    try:
        # Передаем версию напрямую в ChromeDriverManager
        service = Service(executable_path=ChromeDriverManager(driver_version=driver_version).install())
        driver = webdriver.Chrome(service=service, options=options)
        return driver
    except Exception as e:
        print(f"Критическая ошибка при инициализации драйвера: {e}")
        print("\nУбедитесь, что версия CHROME_DRIVER_VERSION в .env файле соответствует версии вашего Яндекс.Браузера.")
        return None
    

def get_page_soup_selenium(driver, url):
    """Загружает страницу с помощью Selenium и возвращает BeautifulSoup объект."""
    if not driver:
        print("Драйвер не инициализирован. Пропуск загрузки страницы.")
        return None
    driver.get(url)
    time.sleep(random.uniform(2, 4))
    return BeautifulSoup(driver.page_source, 'lxml')


def get_category_links(driver):
    """Собирает все уникальные ссылки на категории товаров."""
    catalog_url = f"{BASE_URL}/catalog/"
    soup = get_page_soup_selenium(driver, catalog_url)
    
    if not soup:
        return []
        
    links = set()
    all_a_tags = soup.find_all('a', href=True)
    for tag in all_a_tags:
        href = tag.get('href')
        if href and href.startswith('/catalog/') and href.count('/') == 3 and href.split('/')[-2].isdigit():
            links.add(BASE_URL + href)
    print(f"Найдено {len(links)} уникальных ТОП-уровневых категорий.")
    return list(links)


def parse_product_card(card_soup):
    """
    Парсит ОДНУ карточку товара, извлекая всю доступную информацию.
    (Ваша логика парсинга здесь оставлена без изменений, т.к. она корректна)
    """
    data = {
        'url': None, 'title': None, 'product_id': None, 'gold_price': None,
        'retail_price': None, 'unit': None, 'categories': [], 'features': {},
        'raw_html': str(card_soup)
    }

    # Ссылка
    link_tag = card_soup.find('a', attrs={'data-test': 'product-link'})
    if link_tag and link_tag.has_attr('href'):
        data['url'] = BASE_URL + link_tag['href']
    
    # Название
    title_tag = card_soup.find('span', attrs={'data-test': 'product-title'})
    if title_tag:
        data['title'] = title_tag.text.strip()
    
    # ID товара
    code_tag = card_soup.find('p', attrs={'data-test': 'product-code'})
    if code_tag:
        try:
            # ID может быть не только числовым, например, артикул, поэтому лучше хранить как строку
            product_id_text = code_tag.text.strip()
            if product_id_text.isdigit():
                data['product_id'] = int(product_id_text)
            else:
                 data['product_id'] = product_id_text
        except (ValueError, IndexError):
            pass

    # Цены и единица измерения
    gold_price_tag = card_soup.find('p', attrs={'data-test': 'product-gold-price'})
    if gold_price_tag:
        price_str = gold_price_tag.get_text(strip=True).replace('₽', '').replace('\u2009', '').replace(',', '.')
        try: data['gold_price'] = float(price_str)
        except (ValueError, TypeError): pass

    retail_price_tag = card_soup.find('p', attrs={'data-test': 'product-retail-price'})
    if retail_price_tag:
        price_str = retail_price_tag.get_text(strip=True).replace('₽', '').replace('\u2009', '').replace(',', '.')
        try: data['retail_price'] = float(price_str)
        except (ValueError, TypeError): pass
            
    active_unit_tag = card_soup.find('div', class_=lambda c: c and 'tab-active' in c and 'price-switcher-tab' in c)
    if active_unit_tag:
        data['unit'] = active_unit_tag.get_text(strip=True)

    # Категории (хлебные крошки)
    breadcrumbs_div = card_soup.find('div', attrs={'data-test': 'product-breadcrumbs'})
    if breadcrumbs_div:
        data['categories'] = [cat.get_text(strip=True) for cat in breadcrumbs_div.find_all('a')]

    # Характеристики
    description_p = card_soup.find('p', attrs={'data-test': 'product-description'})
    if description_p:
        for br in description_p.find_all('br'): br.replace_with('|||')
        full_text = description_p.get_text(strip=True)
        features_list = [item.strip() for item in full_text.split('|||') if item.strip()]
        for feature_item in features_list:
            if ':' in feature_item:
                key, value = feature_item.split(':', 1)
                data['features'][key.strip()] = value.strip()
    
    # Сериализация для записи в БД
    data['features'] = json.dumps(data['features'], ensure_ascii=False)
    data['categories'] = json.dumps(data['categories'], ensure_ascii=False)

    return data
