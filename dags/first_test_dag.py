from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import os


@dag(
    dag_id='my_first_test_pipeline',
    start_date=datetime(2026, 6, 1),
    schedule='0 0 * * *',
    catchup=False,
    tags=['learning']
)
def test_pipeline():
    @task
    def extract_step():
        """Шаг 1: Полноценный сборщик данных с обходом страниц"""
        import requests
        import pandas as pd
        import time
        import random
        import logging
        from parser_tools import parse_page

        # Настраиваем логирование внутри Airflow, чтобы видеть инфо в логах таска
        logger = logging.getLogger("airflow.task")

        logger.info("=== ЗАПУСК БОЛЬШОГО СБОРЩИКА ДАННЫХ В AIRFLOW ===")

        total_dataset = []

        # Увеличиваем лимит страниц.
        pages_to_parse = 10
        logger.info(f"Установлено страниц для парсинга: {pages_to_parse}")

        # Маскировка (Headers)
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }

        with requests.Session() as session:
            session.headers.update(headers)  # Добавляем юзер-агента на всю сессию

            for page in range(1, pages_to_parse + 1):
                logger.info(f"Парсим страницу {page} из {pages_to_parse}...")

                # Вызываем твою функцию парсинга страницы
                page_results = parse_page(page, session)

                if page_results is not None and not page_results.empty:
                    total_dataset.append(page_results)
                    logger.info(f"Успешно собрано со страницы {page}: {len(page_results)} объектов")
                else:
                    logger.warning(f"Страница {page} вернула пустой результат.")

                # Пауза между страницами
                if page < pages_to_parse:
                    delay = random.uniform(2.0, 5.0)  # Установка диапазона дилея
                    logger.info(f"Ожидание {delay:.2f} сек перед следующей страницей...")
                    time.sleep(delay)

        if not total_dataset:
            logger.error("Все страницы вернули пустой результат. Выход.")
            raise ValueError("Данные не были собраны.")

        # Собираем всё в один DataFrame
        final_df = pd.concat(total_dataset, ignore_index=True)
        logger.info(f"Сбор окончен. Всего объектов со всех страниц собрано: {len(final_df)}")

        # Путь к файлу обмена для Airflow
        output_path = '/tmp/mirkvartir_raw.csv'

        # Сохраняем в CSV
        final_df.to_csv(output_path, index=False, encoding="utf-8-sig")
        logger.info(f"Данные успешно выгружены в промежуточный файл: {output_path}")

        return output_path

    @task
    def transform_step(file_path: str):
        print(f"=== Шаг 2: Читаем данные из файла {file_path} ===")
        df = pd.read_csv(file_path)

        # Очищаем дубликаты по ссылкам перед заливкой
        df = df.drop_duplicates(subset=['link'])

        pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')


        # Создаем расширенную таблицу под твой реальный датасет
        create_table_query = """
                CREATE TABLE IF NOT EXISTS scraped_realty (
                    id SERIAL PRIMARY KEY,
                    total_price BIGINT,          -- Полная стоимость квартиры
                    price_per_meter BIGINT,      -- Цена за кв. метр
                    calculated_area NUMERIC(5,2), -- Площадь
                    link TEXT UNIQUE,            -- Уникальная ссылка
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
        pg_hook.run(create_table_query)

        # 1. Создаем расширенную таблицу под датасет
        create_table_query = """
            CREATE TABLE IF NOT EXISTS scraped_realty (
                id SERIAL PRIMARY KEY,
                total_price BIGINT,          -- Полная стоимость квартиры
                price_per_meter BIGINT,      -- Цена за кв. метр
                calculated_area NUMERIC(5,2), -- Площадь (например, 54.30)
                link TEXT UNIQUE,            -- Уникальная ссылка
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        pg_hook.run(create_table_query)

        # 2. Временная таблица
        engine = pg_hook.get_sqlalchemy_engine()
        df.to_sql(
            name='temp_scraped_realty',
            con=engine,
            if_exists='replace',
            index=False
        )

        # 3. Делаем UPSERT: если квартира уже есть, обновляем все её метрики
        upsert_query = """
            INSERT INTO scraped_realty (total_price, price_per_meter, calculated_area, link)
            SELECT total_price, price_per_meter, calculated_area, link FROM temp_scraped_realty
            ON CONFLICT (link) 
            DO UPDATE SET 
                total_price = EXCLUDED.total_price,
                price_per_meter = EXCLUDED.price_per_meter,
                calculated_area = EXCLUDED.calculated_area,
                created_at = CURRENT_TIMESTAMP;
            """
        pg_hook.run(upsert_query)

        # 4. Чистим временный объект
        pg_hook.run("DROP TABLE IF EXISTS temp_scraped_realty;")
        print("=== Расширенные данные успешно синхронизированы в Postgres! ===")

    # Теперь по цепочке передается не массив данных, а путь к файлу '/tmp/mirkvartir_raw.csv'
    file_csv_path = extract_step()
    transform_step(file_csv_path)


dag_instance = test_pipeline()