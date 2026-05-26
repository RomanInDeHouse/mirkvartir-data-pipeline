import logging
from urllib.parse import quote

TARGET_CITY = "Москва"
ENCODED_CITY = quote(TARGET_CITY)
BASE_URL = f'https://www.mirkvartir.ru/{ENCODED_CITY}/'

PAGE_TO_PARSE = 3
CSV_FILENAME = "mirkvartir_data.csv"

DELAY_MIN = 3.0
DELAY_MAX = 5.0

USER_AGENTS = [
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"},
    {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15"},
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0"}
]


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("parser.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)


logger = logging.getLogger("mirkvartir_parser")