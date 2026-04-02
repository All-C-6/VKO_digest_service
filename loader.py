"""
Модуль нужен для загрузки всех данных разом с учетом загруженных ранее данных
"""

import json
from datetime import datetime, timedelta
from logging import getLogger
import yaml

from kremlin_handler import get_latest_kremlin_docs, get_webpage_as_xml_tree
from cbr_handler import get_central_bank_draft_regulatory_acts, get_latest_cbr_docs, get_latest_cbr_news
from llm_summarizer import filter_valid_news_and_docs, get_list_summary, extract_items_from_llm_answer
from roskazna_handler import get_latest_roskazna_docs
from ach_handler import get_latest_ach_docs
from utils import setup_logging, save_list_dict_to_excel, convert_data_to_md, convert_data_to_yaml, save_yaml

logger = getLogger(__name__)
setup_logging(log_file_path="logs/loader.log", level="INFO")


def get_news_and_docs_by_timerange(start_date = datetime.today() - timedelta(days=14), end_date = datetime.today()) -> list[dict]:

    # все функции возвращают данные в едином виде
    all_news_and_docs = []

    all_news_and_docs.extend(get_latest_kremlin_docs(start_date, end_date))
    all_news_and_docs.extend(get_latest_cbr_docs(start_date))
    all_news_and_docs.extend(get_latest_cbr_news(start_date))
    all_news_and_docs.extend(get_latest_roskazna_docs(start_date))
    all_news_and_docs.extend(get_latest_ach_docs(start_date))

    return all_news_and_docs


if __name__ == "__main__":
  
    start = start_date=datetime.today() - timedelta(days=22)
    end = start_date=datetime.today()
    all_items = get_news_and_docs_by_timerange()
    save_list_dict_to_excel(all_items, f"all_items_{datetime.strftime(start, "%d.%m.%Y")}-{datetime.strftime(end, "%d.%m.%Y")}.xlsx")

    all_items_yaml_string = convert_data_to_yaml(all_items, "all_items")


    prompt = """
    Оцени, какие из этих новостей и документов жизненно необходимы для учета изменений в нормативных актах и действиях регуляторов в отношнии банков, а также важнейших макроэкономических обстоятельств. Учитывай только важные изменения.
    Формат вывода: ID - заголовок новости/документа.

    """
    summary_result = get_list_summary(
        string_with_data=all_items_yaml_string,
        prompt=prompt,
    )
    
    with open("list_summary.md", 'w') as list_summary_file:
        list_summary_file.write(summary_result)

    id_set = extract_items_from_llm_answer(summary_result)
    
    print(id_set)
    filtered_items = filter_valid_news_and_docs(all_items, id_set)
    filtered_items_yaml_string = convert_data_to_yaml(filtered_items, "filtered_data.yaml")

