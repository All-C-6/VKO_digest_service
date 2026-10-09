"""
Модуль нужен для загрузки всех данных разом с учетом загруженных ранее данных
"""

import json
from datetime import datetime, timedelta
from logging import getLogger
import os
from pathlib import Path
from datetime import datetime, timezone
from telethon import TelegramClient
import requests
import yaml
from dotenv import load_dotenv

from kremlin_handler import get_latest_kremlin_docs, get_webpage_as_xml_tree
from cbr_handler import get_central_bank_draft_regulatory_acts, get_latest_cbr_docs, get_latest_cbr_news
from llm_summarizer import filter_valid_news_and_docs, get_list_summary, extract_items_from_llm_answer, summarize_single_news
from roskazna_handler import get_latest_roskazna_docs
from ach_handler import get_latest_ach_docs
from tg_handler import get_channel_posts_by_dates, get_latest_cbr_posts
from utils import generate_qr_code, get_elements_from_yaml, save_valid_sert, setup_logging, save_list_dict_to_excel, convert_data_to_md, convert_data_to_yaml, save_yaml

logger = setup_logging(log_file_path="logs/loader.log", level="INFO")
load_dotenv()
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
CB_USERNAME = os.getenv("CB_USERNAME")


def get_news_and_docs_by_timerange(start_date = datetime.today() - timedelta(days=14), end_date = datetime.today()) -> list[dict]:
    """
    Получение всех последних данных из разных источников
    """

    # проверка доступности сайтов
    try:
        get_latest_roskazna_docs(datetime.today() - timedelta(days=1))
    except requests.exceptions.SSLError as ssl_error:
        logger.warning(f"Не совпал SSL сертификат для roskazna.gov.ru: {ssl_error}")
        save_valid_sert()

    # все функции возвращают данные в едином виде
    all_news_and_docs = []

    #all_news_and_docs.extend(get_latest_kremlin_docs(start_date, end_date))
    all_news_and_docs.extend(get_latest_cbr_docs(start_date))
    all_news_and_docs.extend(get_latest_cbr_news(start_date))
    all_news_and_docs.extend(get_latest_cbr_posts(start_date, end_date, CB_USERNAME, API_ID, API_HASH))
    all_news_and_docs.extend(get_latest_roskazna_docs(start_date))
    all_news_and_docs.extend(get_latest_ach_docs(start_date))

    return all_news_and_docs


if __name__ == "__main__":
    
    # параметры для загрузки и поиска
    qr_code_path = "qrs"
    prompt = """
    Роль: Аналитик регуляторных рисков и макроэкономики банковского сектора.

    Задача: Проведи фильтрацию входящего списка новостей и документов. Отбери ТОЛЬКО критически значимые события федерального/системного уровня.

    Критерии отбора (ВКЛЮЧАТЬ):
        1. Пруденциальное регулирование: ключевые решения ЦБ РФ/Минфина/правительства, напрямую влияющие на системно значимые кредитные организации (СЗКО).
        2. Макроэкономика: решения по ключевой ставке, фундаментальные изменения налогообложения финансового сектора, валютного курса, долгового рынка и т.д.
        3. Официальные прогнозы и выступления/интервью государственных ведомств.
        4. Топ-кадры: кадровые перестановки только уровня руководства регуляторов (ЦБ, Минфин, ФАС).

    Критерии отсечения (ИСКЛЮЧАТЬ):
        - Неясные изменения с недостаточным количеством подробностей.
        - Внутренние нормативы и правила государственных ведомств.
        - Направленные явно не на системообразующие банки (МФО, строительные холдинги и т.д.).
        - Любые региональные, муниципальные и точечные отраслевые изменения (промышленность, ритейл, АПК и т.д., если они не связаны напрямую с банковской системой в целом).
        - Кадровые изменения среднего звена или в региональных подразделениях.
        - Аналитические прогнозы и мнения экспертов, опросы и информационный шум.

    Формат вывода:
        - Только список отобранных пунктов строго по шаблону: ID - заголовок новости/документа
        - Если ни один документ не прошел фильтр, выведи: «Нет критически значимых изменений».
        - Запрещены любые вводные слова, рассуждения, примечания и оговорки о потенциальной пользе.
    """
    start = datetime.today() - timedelta(days=14)
    end = datetime.today()

    # загрузка данных
    logger.info(f"Идет загрузка новостей за период {start.strftime("%d.%m.%Y")} - {end.strftime("%d.%m.%Y")}")
    all_items = get_news_and_docs_by_timerange(start, end)
    logger.info(f"Загрузка завершена. Загружено {len(all_items)} новостей и документов.")

    # конвертация данных в YAML
    all_items_yaml_string = convert_data_to_yaml(all_items, "all_items")

    # получение сводного описания
    logger.info("Идет получение сводного описания...")
    summary_result = get_list_summary(
        string_with_data=all_items_yaml_string,
        prompt=prompt,
    )
    logger.info("Получение сводного описания завершено.")
    
    with open("list_summary.md", 'w') as list_summary_file:
        list_summary_file.write(summary_result)

    id_set = extract_items_from_llm_answer(summary_result)
    
    logger.info(f"Идентификаторы интересующих новостей: {id_set}")

    filtered_items = filter_valid_news_and_docs(all_items, id_set)
    summarized_filtered_items = [summarize_single_news(item) for item in filtered_items]
    filtered_items_yaml_string = convert_data_to_yaml(summarized_filtered_items, "summarized_filtered_data.yaml")
    links = get_elements_from_yaml("link", "summarized_filtered_data.yaml")

    
    # создание папки для сохранения QR-кодов
    logger.info("Создание папки для сохранения QR-кодов...")
    if not os.path.exists(qr_code_path):
        os.makedirs(qr_code_path)

    qr_code_path = Path(qr_code_path)
    logger.info("Папка для QR-кодов создана.")
    
    # удаление старых QR-кодов
    logger.info("Удаление старых QR-кодов...")
    for file in qr_code_path.iterdir():
        if file.is_file():
            file.unlink()

    logger.info("Удаление старых QR-кодов завершено.")

    # создание новых QR-кодов
    logger.info("Создание новых QR-кодов...")
    for key, link in links.items():
        generate_qr_code(link, f"{qr_code_path}/{datetime.today().strftime("%Y-%m-%d")}_{key}.png")
    logger.info(f"Создано {len(links)} QR-кодов.")

