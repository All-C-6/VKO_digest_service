"""
Модуль нужен для загрузки всех данных разом с учетом загруженных ранее данных
"""

import json
from datetime import datetime, timedelta
from logging import getLogger
import os
from pathlib import Path
import yaml

from kremlin_handler import get_latest_kremlin_docs, get_webpage_as_xml_tree
from cbr_handler import get_central_bank_draft_regulatory_acts, get_latest_cbr_docs, get_latest_cbr_news
from llm_summarizer import filter_valid_news_and_docs, get_list_summary, extract_items_from_llm_answer
from roskazna_handler import get_latest_roskazna_docs
from ach_handler import get_latest_ach_docs
from utils import generate_qr_code, get_elements_from_yaml, setup_logging, save_list_dict_to_excel, convert_data_to_md, convert_data_to_yaml, save_yaml

logger = setup_logging(log_file_path="logs/loader.log", level="INFO")


def get_news_and_docs_by_timerange(start_date = datetime.today() - timedelta(days=14), end_date = datetime.today()) -> list[dict]:

    # все функции возвращают данные в едином виде
    all_news_and_docs = []

    #all_news_and_docs.extend(get_latest_kremlin_docs(start_date, end_date))
    all_news_and_docs.extend(get_latest_cbr_docs(start_date))
    all_news_and_docs.extend(get_latest_cbr_news(start_date))
    all_news_and_docs.extend(get_latest_roskazna_docs(start_date))
    all_news_and_docs.extend(get_latest_ach_docs(start_date))

    return all_news_and_docs


if __name__ == "__main__":
    
    # параметры для загрузки и поиска
    qr_code_path = "qrs"
    prompt = """
    Оцени, какие из этих новостей и документов жизненно необходимы для учета изменений в нормативных актах и действиях регуляторов в отношнии системообразующих крупных банков, а также важнейшие изменения в экономике.
    Выводи только в формате: ID - заголовок новости/документа. 
    
    Выводи только гарантированно нужные документы без дополнений про то, что может быть полезно.
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
    filtered_items_yaml_string = convert_data_to_yaml(filtered_items, "filtered_data.yaml")
    links = get_elements_from_yaml("link", "filtered_data.yaml")

    
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

