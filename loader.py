"""
Модуль нужен для загрузки всех данных разом с учетом загруженных ранее данных
"""

import json
from datetime import datetime, timedelta

import yaml

from kremlin_handler import get_latest_kremlin_docs, get_webpage_as_xml_tree
from cbr_handler import get_central_bank_draft_regulatory_acts, get_latest_cbr_docs, get_latest_cbr_news
from llm_summarizer import filter_valid_news_and_docs, get_list_summary, extract_items_from_llm_answer
from roskazna_handler import get_latest_roskazna_docs
from ach_handler import get_latest_ach_docs
from utils import setup_logging, save_list_dict_to_excel, convert_data_to_md, convert_data_to_yaml


def get_news_and_docs_by_timerange(start_date = datetime.today() - timedelta(days=14), end_date = datetime.today()):

    # все функции возвращают данные в едином виде
    all_news_and_docs = []

    all_news_and_docs.extend(get_latest_kremlin_docs(start_date, end_date))
    all_news_and_docs.extend(get_latest_cbr_docs(start_date))
    all_news_and_docs.extend(get_latest_cbr_news(start_date))
    all_news_and_docs.extend(get_latest_roskazna_docs(start_date))
    all_news_and_docs.extend(get_latest_ach_docs(start_date))

    return all_news_and_docs


if __name__ == "__main__":
    """
    start = start_date=datetime.today() - timedelta(days=10)
    end = start_date=datetime.today()
    all_docs = get_news_and_docs_by_timerange()
    save_list_dict_to_excel(all_docs, f"all_docs_{datetime.strftime(start, "%d.%m.%Y")}-{datetime.strftime(end, "%d.%m.%Y")}.xlsx")
    yaml_string = convert_data_to_yaml(all_docs, "all_docs")
    print(yaml_string)
    """

    prompt = """
    Оцени, какие из этих новостей и документов необходимы ответственным лицам банков для учета изменений в нормативных актах и действиях регуляторов в отношнии банков, а также важнейших макроэкономических обстоятельств. Учитывай только изменения.
    Формат вывода: ID - заголовок новости/документа.

    """
    """
    summary_result = get_list_summary(
        string_with_data=yaml_string,
        prompt=prompt,
    )

    print("=== Ответ модели ===")
    print(summary_result)
    """
    with open("list_summary.md", 'r') as list_summary_file:
        answer = list_summary_file.read()

    id_set = extract_items_from_llm_answer(answer)
    print(id_set)
    
    with open('all_docs.yaml', 'r', encoding='utf-8') as file:
        # Загружаем данные с помощью safe_load
        data = yaml.safe_load(file)

    valid_docs_and_news = filter_valid_news_and_docs(data, id_set)
    
    print([item["title"] for item in valid_docs_and_news])