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

def parse_product_card(card_soup):
    # ... Эта функция остается АБСОЛЮТНО без изменений, как в прошлом сообщении ...
    # (Скопируй ее сюда из предыдущего ответа)
    data = {'url': None,'title': None,'product_id': None,'gold_price': None, 'retail_price': None, 'unit': None,'categories': [],'features': {}, 'raw_html': str(card_soup)}
    link_tag = card_soup.find('a', attrs={'data-test': 'product-link'}); title_tag = card_soup.find('span', attrs={'data-test': 'product-title'}); code_tag = card_soup.find('p', attrs={'data-test': 'product-code'})
    if link_tag and link_tag.has_attr('href'): data['url'] = BASE_URL + link_tag['href']
    if title_tag: data['title'] = title_tag.text.strip()
    if code_tag:
        try: data['product_id'] = int(code_tag.text.strip())
        except: pass
    gold_price_tag = card_soup.find('p', attrs={'data-test': 'product-gold-price'})
    if gold_price_tag:
        price_str = gold_price_tag.get_text(strip=True).replace('₽', '').replace('\u2009', '').replace(',', '.');_ = price_str
        try: data['gold_price'] = float(price_str)
        except: pass
    retail_price_tag = card_soup.find('p', attrs={'data-test': 'product-retail-price'})
    if retail_price_tag:
        price_str = retail_price_tag.get_text(strip=True).replace('₽', '').replace('\u2009', '').replace(',', '.');_ = price_str
        try: data['retail_price'] = float(price_str)
        except: pass
    active_unit_tag = card_soup.find('div', class_=lambda c: c and 'tab-active' in c and 'price-switcher-tab' in c)
    if active_unit_tag: data['unit'] = active_unit_tag.get_text(strip=True)
    breadcrumbs_div = card_soup.find('div', attrs={'data-test': 'product-breadcrumbs'})
    if breadcrumbs_div: data['categories'] = [cat.get_text(strip=True) for cat in breadcrumbs_div.find_all('a')]
    description_p = card_soup.find('p', attrs={'data-test': 'product-description'})
    if description_p:
        for br in description_p.find_all('br'): br.replace_with('|||')
        features_list = [item.strip() for item in description_p.get_text(strip=True).split('|||') if item.strip()]
        for feature_item in features_list:
            if ':' in feature_item: key, value = feature_item.split(':', 1); data['features'][key.strip()] = value.strip()
    data['features'] = json.dumps(data['features'], ensure_ascii=False)
    data['categories'] = json.dumps(data['categories'], ensure_ascii=False)
    return data
