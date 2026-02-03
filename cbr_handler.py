"""
Модуль преднзначен для извлечения данных с сайта Центробанка РФ.
С помощью RSS получается список документов, извлекается набор с датами позднее заданной.
Другая функция сохраняет PDF файлы по ссылкам.
"""

import requests
from pathlib import Path
from urllib.parse import unquote
import re
import xml.etree.ElementTree as ET
import logging
from datetime import datetime

from utils import setup_logging

logger = logging.getLogger(__name__)
setup_logging(log_file_path="logs/cbr_handler.log", level="INFO")


def download_cbr_pdf(url: str, destination_directory: str = ".") -> str:
    """
    Скачивает PDF файл по URL и сохраняет под оригинальным именем.

    Может скачивать любые PDF, не только от ЦБ РФ.

    Имя файла берется из Content-Disposition, если оно есть там, или из URL.

    Args:
        url: URL файла для скачивания (может быть с протоколом или без)
        destination_directory: Директория для сохранения файла (по умолчанию текущая)

    Returns:
        str: Полный путь к сохраненному файлу

    Raises:
        requests.exceptions.RequestException: При ошибках сетевого запроса
        ValueError: Если не удалось определить имя файла
    """
    # Добавляем протокол, если его нет
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url

    # Выполняем запрос с allow_redirects для получения финального URL
    response = requests.get(url, allow_redirects=True, stream=True)
    response.raise_for_status()

    # Пытаемся получить имя файла из заголовка Content-Disposition
    original_filename = None
    if 'Content-Disposition' in response.headers:
        content_disposition = response.headers['Content-Disposition']

        # Приоритет 1: filename*=utf-8''имя_файла (RFC 5987)
        utf8_match = re.search(r"filename\*=utf-8''([^;]+)", content_disposition, re.IGNORECASE)
        if utf8_match:
            original_filename = unquote(utf8_match.group(1))

        # Приоритет 2: filename="имя_файла" или filename=имя_файла
        if not original_filename:
            filename_match = re.search(r'filename=(["\']?)([^;"\'"]+)\1', content_disposition, re.IGNORECASE)
            if filename_match:
                original_filename = unquote(filename_match.group(2))

    # Если не удалось получить из заголовка, пытаемся из URL
    if not original_filename:
        # Берем последнюю часть URL после редиректов
        url_path = response.url.split('?')[0]  # Убираем query параметры
        original_filename = url_path.split('/')[-1]

        # Если имя пустое или подозрительное, генерируем из ID
        if not original_filename or len(original_filename) < 3:
            # Берем ID из исходного URL
            url_parts = url.rstrip('/').split('/')
            file_id = url_parts[-1] if url_parts else 'downloaded'
            original_filename = f"{file_id}.pdf"

    # Убираем лишние расширения .pdf, если их несколько
    original_filename = re.sub(r'(\.pdf)+$', '.pdf', original_filename, flags=re.IGNORECASE)

    # Если расширения нет вообще, добавляем .pdf
    if not original_filename.lower().endswith('.pdf'):
        original_filename += '.pdf'

    # Создаем директорию, если не существует
    destination_path = Path(destination_directory)
    destination_path.mkdir(parents=True, exist_ok=True)

    # Полный путь к файлу
    full_file_path = destination_path / original_filename

    # Сохраняем файл
    with open(full_file_path, 'wb') as file:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                file.write(chunk)

    print(f"Файл успешно скачан: {full_file_path}")
    return str(full_file_path)


def get_latest_cbr_docs(start_date: datetime) -> list[dict[str, str]]:
    """
    Получает список документов ЦБ РФ, опубликованных позднее указанной даты-времени.

    Функция парсит RSS-ленту нормативных актов Банка России и возвращает массив
    словарей с информацией о каждом документе, опубликованном после указанной даты.

    Args:
        target_datetime (datetime): Дата-время, позднее которой нужно искать документы.
                                   Должна быть в timezone-aware формате или UTC.

    Returns:
        List[Dict[str, str]]: Список словарей, каждый из которых содержит:
            - 'title': Заголовок документа
            - 'link': Ссылка на документ
            - 'guid': Уникальный идентификатор документа  
            - 'description': Описание документа
            - 'pub_date': Дата публикации в строковом формате
            - 'pub_date_parsed': Распарсенная дата публикации в ISO формате

    """
    cbr_rss_url = "https://www.cbr.ru/rss/navr"
    resulting_documents_list = []

    logger.info(f"Начинаем получение документов ЦБ РФ позднее {start_date.isoformat()}")

    try:
        # Выполняем запрос к RSS-ленте ЦБ РФ
        logger.debug(f"Отправляем запрос к {cbr_rss_url}")
        response = requests.get(cbr_rss_url, timeout=30)
        response.raise_for_status()

        logger.debug(f"Получен ответ с кодом {response.status_code}, размер: {len(response.content)} байт")

        # Парсим XML содержимое
        xml_root_element = ET.fromstring(response.content)

        # Находим все элементы item в RSS
        rss_items = xml_root_element.findall('.//item')
        logger.info(f"Найдено {len(rss_items)} элементов в RSS-ленте")

        for item_element in rss_items:
            try:
                # Извлекаем данные из каждого элемента
                document_title = item_element.find('title')
                document_link = item_element.find('link') 
                document_guid = item_element.find('guid')
                document_description = item_element.find('description')
                document_pub_date = item_element.find('pubDate')

                # Проверяем наличие обязательных полей
                if any(field is None for field in [document_title, document_link, document_pub_date]):
                    logger.warning("Пропускаем элемент с отсутствующими обязательными полями")
                    continue

                publication_date_string = document_pub_date.text.strip()

                # Парсим дату публикации (формат: "Mon, 29 Dec 2025 16:43:00 +0300")
                parsed_publication_datetime = datetime.strptime(
                    publication_date_string, 
                    "%a, %d %b %Y %H:%M:%S %z"
                ).replace(tzinfo=None)

                # Проверяем, что документ опубликован позднее целевой даты
                if parsed_publication_datetime > start_date:
                    document_info = {
                        'title': document_title.text.strip(),
                        'link': document_link.text.strip(),
                        'guid': document_guid.text.strip() if document_guid is not None else '',
                        'description': document_description.text.strip() if document_description is not None else '',
                        'pub_date': publication_date_string,
                        'pub_date_parsed': parsed_publication_datetime.isoformat()
                    }

                    resulting_documents_list.append(document_info)
                    logger.debug(f"Добавлен документ: {document_title.text.strip()[:100]}...")
                else:
                    logger.debug(f"Документ пропущен (дата {parsed_publication_datetime} <= {start_date})")

            except ValueError as date_parsing_error:
                logger.error(f"Ошибка парсинга даты в элементе: {date_parsing_error}")
                continue
            except Exception as item_processing_error:
                logger.error(f"Ошибка обработки элемента RSS: {item_processing_error}")
                continue

        logger.info(f"Обработка завершена. Найдено {len(resulting_documents_list)} документов позднее {start_date.isoformat()}")
        return resulting_documents_list

    except requests.RequestException as network_error:
        logger.error(f"Ошибка сетевого запроса к ЦБ РФ: {network_error}")
        raise
    except ET.ParseError as xml_parsing_error:
        logger.error(f"Ошибка парсинга XML от ЦБ РФ: {xml_parsing_error}")
        raise
    except Exception as unexpected_error:
        logger.error(f"Неожиданная ошибка при получении документов ЦБ РФ: {unexpected_error}")
        raise


def get_central_bank_draft_regulatory_acts() -> list[dict]:
    """
    Получает последние проекты нормативных актов (НПА) от Центрального Банка РФ.

    Returns:
        List[Dict[str, Optional[str]]]: Массив словарей с информацией о проектах НПА.
            Каждый словарь содержит поля:
            - title: название проекта
            - link: ссылка на проект
            - guid: уникальный идентификатор
            - description: описание проекта
            - pub_date: дата публикации (строка)
            - category: категория/департамент

    Raises:
        requests.RequestException: Ошибка при получении данных с сервера
        ET.ParseError: Ошибка при парсинге XML
    """
    rss_feed_url = "https://www.cbr.ru/rss/project"

    try:
        # Получаем XML данные
        response = requests.get(rss_feed_url, timeout=10)
        response.raise_for_status()
        response.encoding = 'utf-8'

        # Парсинг XML
        root = ET.fromstring(response.content)

        # Определяем namespace для atom (здесь не нужно, возможно, понадобится в других случаях)
        #namespaces = {
        #    'atom': 'http://www.w3.org/2005/Atom'
        #}

        # Находим все элементы item и заполняем массив словарей
        items = root.findall('.//item')
        draft_regulatory_acts_list = []
        for item in items:
            title_element = item.find('title')
            link_element = item.find('link')
            guid_element = item.find('guid')
            description_element = item.find('description')
            pub_date_element = item.find('pubDate')
            category_element = item.find('category')

            draft_regulatory_act_info = {
                'title': title_element.text if title_element is not None else None,
                'link': link_element.text if link_element is not None else None,
                'guid': guid_element.text if guid_element is not None else None,
                'description': description_element.text if description_element is not None else None,
                'pub_date': pub_date_element.text if pub_date_element is not None else None,
                'category': category_element.text if category_element is not None else None,
            }

            draft_regulatory_acts_list.append(draft_regulatory_act_info)

        return draft_regulatory_acts_list

    except requests.RequestException as request_error:
        print(f"Ошибка при получении данных с сервера ЦБ РФ: {request_error}")
    except ET.ParseError as parse_error:
        print(f"Ошибка при парсинге XML: {parse_error}")
    except Exception as e_other:
        print(f"Иная ошибка: {e_other}")