import random
import re
import time
import bs4
from bs4 import BeautifulSoup
import pandas as pd
import requests


import config
from config import logger


def get_total_pages(session):
    """Динамически определяет количество страниц на сайте"""
    headers = random.choice(config.USER_AGENTS)
    try:
        response = session.get(config.BASE_URL, headers=headers, timeout=10)
        if response.status_code != 200:
            logger.warning(f"Не удалось получить главную страницу для подсчета страниц. Статус: {response.status_code}")
            return 1

        soup = BeautifulSoup(response.text, 'html.parser')


        pagination = soup.find('div', class_=re.compile(r'pagination', re.I)) or soup.find('nav')
        if not pagination:
            logger.warning("Блок пагинации не найден. Парсим 1 страницу.")
            return 1

        links = pagination.find_all('a')
        pages = []
        for link in links:
            text = link.text.strip()
            if text.isdigit():
                pages.append(int(text))

        if pages:
            total_pages = max(pages)
            return min(total_pages, 400) # Лимит по количеству страниц

        return 1
    except Exception as e:
        logger.error(f"Ошибка при определении количества страниц: {e}")
        return 1


def parse_page(page_num, session):
    """Сканирует одну страницу и возвращает датафрейм найденных квартир"""
    if page_num == 1:
        url = config.BASE_URL
    else:
        url = f"{config.BASE_URL}?p={page_num}"

    logger.info(f"Сканируем страницу {page_num}: {url}")
    headers = random.choice(config.USER_AGENTS)

    try:
        response = session.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            logger.warning(f"Пропуск страницы {page_num}, статус-код: {response.status_code}")
            return pd.DataFrame()

        soup = BeautifulSoup(response.text, 'html.parser')


        cards = soup.find_all('article', class_='Gmsmb')

        prices_total = []
        prices_per_m = []
        calculated_areas = []
        urls = []

        for card in cards:
            link_tag = card.find('a', href=True)
            if link_tag:
                href = link_tag['href'].strip()
                if href.startswith('/'):
                    href = "https://www.mirkvartir.ru" + href
            else:
                href = "No link"

            # Поиск блока с ценами внутри этой же карточки
            price_box = card.find('div', class_='_NrqU')
            if not price_box:
                continue

            strong_tag = price_box.find('strong')
            small_tag = price_box.find('small')
            if not strong_tag or not small_tag:
                continue

            raw_price_total = re.sub(r'\D', '', strong_tag.text)
            raw_price_m = re.sub(r'\D', '', small_tag.text)

            price_total = int(raw_price_total) if raw_price_total else 0
            price_m = int(raw_price_m) if raw_price_m else 0

            if price_total == 0 or price_m == 0:
                continue

            # Формула по расчету за метр
            area = round(price_total / price_m, 1)


            prices_total.append(price_total)
            prices_per_m.append(price_m)
            calculated_areas.append(area)
            urls.append(href)

        page_data = pd.DataFrame({
            "total_price": prices_total,
            "price_per_meter": prices_per_m,
            "calculated_area": calculated_areas,
            "link": urls
        })

        logger.info(f"-> Успешно собрано объектов со страницы {page_num}: {len(page_data)}")
        return page_data

    except Exception as e:
        logger.error(f"Ошибка при обработке страницы {page_num}: {e}")
        return pd.DataFrame()

def main():
    total_dataset = []

    logger.info("=== ЗАПУСК БОЛЬШОГО СБОРЩИКА ДАННЫХ ===")

    with requests.Session() as session:
        # Устанавливаем лимит на количество парсов!!!
        pages_to_parse = 3
        logger.info(f"Принудительно установлено страниц для парсинга: {pages_to_parse}")

        for page in range(1, pages_to_parse + 1):
            page_results = parse_page(page, session)

            if not page_results.empty:
                total_dataset.append(page_results)

            if page < pages_to_parse:
                delay = random.uniform(config.DELAY_MIN, config.DELAY_MAX)
                logger.info(f"   Ожидание {delay:.2f} сек перед следующей страницей...")
                time.sleep(delay)

    if not total_dataset:
        logger.error("Все страницы вернули пустой результат. Выход.")
        return

    final_df = pd.concat(total_dataset, ignore_index=True)
    logger.info(f"\nСбор окончен. Всего объектов со всех страниц собрано: {len(final_df)}")

    try:
        final_df.to_csv(config.CSV_FILENAME, sep=";", index=False, encoding="utf-8-sig")
        logger.info(f"Данные успешно выгружены в файл: {config.CSV_FILENAME}")
    except Exception as e:
        logger.error(f"Не удалось сохранить CSV-файл: {e}")


