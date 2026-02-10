import requests
import xml.etree.ElementTree as ET
from html.parser import HTMLParser
from datetime import datetime
from typing import List, Dict, Optional
import logging

from utils import setup_logging

# Настройка логирования
logger = logging.getLogger(__name__)
setup_logging(log_file_path="logs/roskazna_handler.log", level="INFO")

CERT_BUNDLE_PATH = './digi-min.pem'
headers = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
}


class TextExtractorHTMLParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.extracted_text_parts = []

    def handle_data(self, data):
        """Обработчик текстовых данных между тегами"""
        stripped_data = data.strip()
        if stripped_data:
            self.extracted_text_parts.append(stripped_data)

    def get_concatenated_text(self) -> str:
        """Возвращает весь извлеченный текст, объединенный пробелами"""
        return ' '.join(self.extracted_text_parts)


def get_latest_roskazna_docs(start_date: datetime) -> List[Dict[str, str]]:
    """
    Получает RSS-ленту новостей с сайта Росказны и возвращает новости,
    опубликованные после указанной даты.

    Args:
        start_date: Дата начала выборки новостей (без учета времени)

    Returns:
        Список словарей с данными новостей, содержащих ключи:
        - title: заголовок новости
        - link: ссылка на новость
        - description: описание новости
        - category: категория новости
        - pub_date: дата публикации (без времени)
    """
    roskazna_rss_url = "https://roskazna.gov.ru/rss"
    filtered_news_items_list = []

    try:
        logger.info(f"Запрос RSS-ленты с {roskazna_rss_url}")

        # Выполняем HTTP-запрос для получения XML
        http_response = requests.get(roskazna_rss_url, timeout=30, verify=CERT_BUNDLE_PATH, headers=headers)
        http_response.raise_for_status()
        http_response.encoding = 'utf-8'

        logger.info(f"RSS-лента успешно получена, размер: {len(http_response.content)} байт")

        # Парсим XML
        xml_root_element = ET.fromstring(http_response.content)

        # Находим все элементы <item> внутри <channel>
        news_items_elements = xml_root_element.findall('.//channel/item')
        logger.info(f"Найдено {len(news_items_elements)} новостей в RSS-ленте")

        # Обрабатываем каждый элемент новости
        for news_item_element in news_items_elements:
            news_item_data_dict = extract_news_item_data(news_item_element, start_date)

            if news_item_data_dict is not None:
                filtered_news_items_list.append(news_item_data_dict)

        logger.info(f"Отфильтровано {len(filtered_news_items_list)} новостей после {start_date.date()}")

    except requests.exceptions.RequestException as request_error:
        logger.error(f"Ошибка при запросе RSS-ленты: {request_error}")
        raise
    except ET.ParseError as xml_parse_error:
        logger.error(f"Ошибка при парсинге XML: {xml_parse_error}")
        raise
    except Exception as unexpected_error:
        logger.error(f"Неожиданная ошибка: {unexpected_error}")
        raise

    return filtered_news_items_list


def extract_news_item_data(
    news_item_element: ET.Element, 
    start_date: datetime
) -> Optional[Dict[str, str]]:
    """
    Извлекает данные из элемента <item> RSS-ленты.

    Args:
        news_item_element: XML элемент <item>
        start_date: Дата начала выборки для фильтрации

    Returns:
        Словарь с данными новости или None, если новость старше start_date
    """
    try:
        # Извлекаем pubDate и парсим дату
        publication_date_string = get_element_text(news_item_element, 'pubDate')

        if publication_date_string:
            # Парсим дату формата "Mon, 02 Feb 2026 13:05:17 +0300"
            publication_datetime = parse_rss_date(publication_date_string)

            # Проверяем, что дата публикации позже или равна start_date
            if publication_datetime.date() < start_date.date():
                return None

            publication_date_only = publication_datetime.strftime('%Y-%m-%d')
        else:
            logger.warning("Новость без даты публикации, пропускаем")
            return None

        # Извлекаем данные из элемента
        news_title = clean_cdata(get_element_text(news_item_element, 'title'))
        news_link = get_element_text(news_item_element, 'link')
        try:
            news_description = get_whole_HTML_element_text(clean_cdata(get_element_text(news_item_element, 'description')))
        except Exception as e:
            print(e)

        news_item_data_dict = {
            'title': news_title,
            'link': news_link,
            'meta': news_description,
            'pub_date': publication_date_only
        }

        return news_item_data_dict

    except Exception as extraction_error:
        logger.error(f"Ошибка при извлечении данных новости: {extraction_error}")
        return None


def get_element_text(parent_element: ET.Element, tag_name: str) -> Optional[str]:
    """
    Безопасно извлекает текст из XML элемента.

    Args:
        parent_element: Родительский XML элемент
        tag_name: Имя тега для поиска

    Returns:
        Текст элемента или None, если элемент не найден
    """
    element = parent_element.find(tag_name)
    return element.text.strip() if element is not None and element.text else None


def clean_cdata(text: str) -> str:
    """
    Очищает текст от CDATA обёрток и лишних пробелов.

    Args:
        text: Исходный текст

    Returns:
        Очищенный текст
    """

    cleaned_text = text.strip()

    # Убираем CDATA обёртки, если они есть
    if cleaned_text.startswith('<![CDATA[') and cleaned_text.endswith(']]>'):
        cleaned_text = cleaned_text[9:-3].strip()

    return cleaned_text


def parse_rss_date(date_string: str) -> datetime:
    """
    Парсит дату из RSS формата в datetime объект.

    Args:
        date_string: Строка с датой в формате RSS (RFC 2822)

    Returns:
        Объект datetime
    """
    # Формат: "Mon, 02 Feb 2026 13:05:17 +0300"
    # Используем %a, %d %b %Y %H:%M:%S %z
    try:
        parsed_datetime = datetime.strptime(date_string, '%a, %d %b %Y %H:%M:%S %z')
        return parsed_datetime
    except ValueError as date_parse_error:
        logger.error(f"Ошибка парсинга даты '{date_string}': {date_parse_error}")
        raise


def get_whole_HTML_element_text(html_string: str) -> str:
    """
    Извлекает весь текстовый контент из HTML разметки, конкатенируя его в порядке следования.

    Args:
        html_string: Строка с HTML разметкой

    Returns:
        Строка с извлеченным текстом без HTML тегов
    """

    parser = TextExtractorHTMLParser()
    parser.feed(html_string)

    return parser.get_concatenated_text()
