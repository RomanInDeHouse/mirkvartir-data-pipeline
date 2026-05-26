import requests
from bs4 import BeautifulSoup
import re
import time
import random
import config
from config import logger
import pandas as pd


def parse_page(page_num, session):
    """Сканирует одну страницу и возвращает DataFrame найденных квартир"""
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

        all_links = soup.find_all('a', href=True)
        flat_urls = set()
        for a in all_links:
            href = a['href'].strip()
            if re.search(r'/\d+/$', href) and not any(x in href for x in ['journal', 'listing', 'novostroyki']):
                flat_urls.add(href)
        flat_urls = list(flat_urls)

        cards = soup.find_all('div', class_='DBYlq')
        prices = []
        for card in cards:
            price_box = card.find('div', class_='_NrqU')
            if price_box:
                raw_price = price_box.text.strip()
                clean_price = raw_price.split('₽')[0].strip() + ' ₽'
                prices.append(clean_price)

        limit = min(len(flat_urls), len(prices))
        valid_urls = flat_urls[:limit]
        valid_prices = prices[:limit]

        titles = []
        for i in range(limit):
            url_parts = [p for p in valid_urls[i].split('/') if p]
            title = url_parts[-2].replace('+', ' ') if len(url_parts) > 2 else "Квартира"
            titles.append(title)

        page_data = pd.DataFrame({
            "title": titles,
            "price": valid_prices,
            "link": valid_urls
        })

        logger.info(f"-> Успешно собрано объектов со страницы {page_num}: {len(page_data)}")
        return page_data

    except Exception as e:
        logger.error(f"Ошибка при обработке страницы {page_num}: {e}")
        return pd.DataFrame()


def get_total_pages(session):
    """Определяет максимальное количество страниц на сайте"""
    logger.info(f"Определяем общее количество страниц на {config.BASE_URL}...")
    headers = random.choice(config.USER_AGENTS)

    try:
        response = session.get(config.BASE_URL, headers=headers, timeout=10)
        if response.status_code != 200:
            logger.error(f"Не удалось получить стартовую страницу для пагинации. Статус: {response.status_code}")
            return 1

        soup = BeautifulSoup(response.text, 'html.parser')

        pagination_links = soup.find_all('a', href=re.compile(r'\?p=\d+'))

        page_numbers = []
        for a in pagination_links:
            href = a['href']
            match = re.search(r'\?p=(\d+)', href)
            if match:
                page_numbers.append(int(match.group(1)))

        if page_numbers:
            max_page = max(page_numbers)
            logger.info(f"=== ОБНАРУЖЕНО СТРАНИЦ ДЛЯ СКАНИРОВАНИЯ: {max_page} ===")
            return max_page

        logger.warning("Блок пагинации не найден на странице. Парсим только страницу 1.")
        return 1

    except Exception as e:
        logger.error(f"Ошибка при определении пагинации: {e}")
        return 1






def main():
    total_dataset = []


    logger.info("=== ЗАПУСК БОЛЬШОГО СБОРЩИКА ДАННЫХ ===")

    with requests.Session() as session:
        pages_to_parse = get_total_pages(session)
        for page in range(1, pages_to_parse + 1):
            page_results = parse_page(page, session)

            if not page_results.empty:
                total_dataset.append(page_results)

            if page < pages_to_parse:
                delay = random.uniform(4.1, 7.2)
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


if __name__ == "__main__":
    main()