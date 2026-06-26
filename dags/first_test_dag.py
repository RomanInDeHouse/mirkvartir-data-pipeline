from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
import io

BUCKET_NAME = "mirkvartir-raw"

@dag(
    dag_id='my_first_test_pipeline_v2',
    start_date=datetime(2026, 6, 1),
    schedule='0 0 * * *',
    catchup=False,
    tags=['learning', 's3']
)
def test_pipeline():
    @task
    def extract_to_s3():
        """Шаг 1: Полноценный сборщик данных с обходом страниц и выгрузкой в S3"""
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
                page_results = parse_page(page, session, logger=logger)

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

        #Генерация уникального имени файла на основе запуска
        execution_date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        s3_key = f"moscow_realty_{execution_date}.csv"

        #Перевод DataFrame в CSV-строку прямо в оперативной памяти(без сохранения на диск)
        csv_buffer = io.StringIO()
        final_df.to_csv(csv_buffer, index=False, encoding="utf-8-sig")

        #Подключение к MinIO через созданные коннект и загрузка CSV строки как файл
        s3_hook = S3Hook(aws_conn_id='my_s3_conn')
        s3_hook.load_string(
            string_data=csv_buffer.getvalue(),
            key=s3_key,
            bucket_name=BUCKET_NAME,
            replace=True
        )

        logger.info(f"Данные успешно отправлены в S3 Data Lake! Имя объекта: {s3_key}")
        return s3_key
    @task
    def transform_from_s3(s3_key: str):
        print(f"=== Шаг 2: Достаем объект {s3_key} из S3 хранилища ===")
        s3_hook = S3Hook(aws_conn_id='my_s3_conn')
        s3_object = s3_hook.get_key(key=s3_key, bucket_name=BUCKET_NAME)
        s3_data = s3_object.get()['Body'].read().decode('utf-8')

        # Превращаем скачанный текст в нормальный Pandas DataFrame
        df = pd.read_csv(io.StringIO(s3_data))

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


        time_suffix = s3_key.replace("moscow_realty_","").replace(".csv", "").replace("-", "_")
        temp_table_name = f"temp_realty_{time_suffix}"

        engine = pg_hook.get_sqlalchemy_engine()

        #Заливка данных в изолированную таблицу
        df.to_sql(
            name=temp_table_name,
            con=engine,
            if_exists='replace',
            index=False


        )

        #  Делаем UPSERT: если квартира уже есть, обновляем все её метрики
        upsert_query = f"""
            INSERT INTO scraped_realty (total_price, price_per_meter, calculated_area, link)
            SELECT total_price, price_per_meter, calculated_area, link FROM {temp_table_name}
            ON CONFLICT (link) 
            DO UPDATE SET 
                total_price = EXCLUDED.total_price,
                price_per_meter = EXCLUDED.price_per_meter,
                calculated_area = EXCLUDED.calculated_area,
                created_at = CURRENT_TIMESTAMP;
            """
        pg_hook.run(upsert_query)

        # 4. Чистим временный объект
        pg_hook.run(f"DROP TABLE IF EXISTS {temp_table_name};")
        print("=== Расширенные данные успешно синхронизированы в Postgres! ===")

    # Теперь по цепочке передается не массив данных, а путь к файлу '/tmp/mirkvartir_raw.csv'
    current_s3_key = extract_to_s3()
    transform_from_s3(current_s3_key)


dag_instance = test_pipeline()