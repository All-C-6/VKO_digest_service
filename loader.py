"""
Модуль нужен для загрузки всех данных разом с учетом загруженных ранее данных
"""

import json
from datetime import datetime, timedelta

from kremlin_handler import get_latest_kremlin_docs, get_webpage_as_xml_tree
from cbr_handler import get_central_bank_draft_regulatory_acts, get_latest_cbr_docs, get_latest_cbr_news
from roskazna_handler import get_latest_roskazna_docs
from ach_handler import get_latest_ach_docs


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
    get_news_and_docs_by_timerange()