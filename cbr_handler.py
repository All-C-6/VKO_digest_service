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
from lxml import html
import logging
from datetime import datetime
import time

from utils import setup_logging, drop_uwanted_symbols, extract_pdf_full_text_advanced

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
        start_date (datetime): Дата-время, позднее которой нужно искать документы.

    Returns:
    
        list (list[Dict[str, str]]): Список словарей, каждый из которых содержит:
            - 'id': Идентификатор документа Банка России
            - 'title': Заголовок документа
            - 'link': Ссылка на документ
            - 'meta': Описание документа
            - 'pub_date': Дата публикации в строковом формате

    """
    cbr_rss_url = "https://www.cbr.ru/rss/navr"
    resulting_documents_list = []

    start_date = start_date.date()
    logger.info(f"Начинаем получение документов ЦБ РФ позднее {start_date}")

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
                ).replace(tzinfo=None).date()

                # Проверяем, что документ опубликован позднее целевой даты
                if parsed_publication_datetime > start_date:
                    document_info = {
                        'id': document_guid.text.strip(),
                        'title': document_title.text.strip(),
                        'link': document_link.text.strip(),
                        'meta': document_description.text.strip() if document_description is not None else '',
                        'pub_date': parsed_publication_datetime.isoformat()
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


def get_central_bank_draft_regulatory_acts(start_date: datetime) -> list[dict]:
    """
    Получает последние проекты нормативных актов (НПА) от Центрального Банка РФ.

    Args:
        start_date (datetime): Дата-время, позднее которой нужно искать проекты документов.
    
    Returns:
        List[Dict[str, Optional[str]]]: Массив словарей с информацией о проектах НПА.
            Каждый словарь содержит поля:
            - title: название проекта
            - link: ссылка на проект
            - id: уникальный идентификатор
            - meta: описание проекта
            - pub_date: дата публикации (строка)

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
            pub_date = datetime.strptime(
                    item.find('pubDate').text, 
                    "%a, %d %b %Y %H:%M:%S %z"
                ).replace(tzinfo=None)
            
            # так как документы идут от более молодых к старым, первый слишком старый будет означать,
            # что и другие нам не интересны
            if pub_date <= start_date:
                break
                
            title_element = item.find('title')
            link_element = item.find('link')
            guid_element = item.find('guid')
            doc_info = ""
            try:
                pdf_url = f"https://www.cbr.ru/Queries/XsltBlock/File/90538/{guid_element.text.split("_")[1]}/note"
            except (ValueError, TypeError) as url_err:
                logger.error(f"Ошибка во время составления ссылки на PDF: {url_err}")
            else:
                doc_info = extract_pdf_full_text_advanced(pdf_url)
            

            draft_regulatory_act_info = {
                'title': title_element.text if title_element is not None else None,
                'link': link_element.text if link_element is not None else None,
                'id': guid_element.text,
                'meta': drop_uwanted_symbols(doc_info),
                'pub_date': pub_date.date().isoformat()
            }

            draft_regulatory_acts_list.append(draft_regulatory_act_info)

        return draft_regulatory_acts_list

    except requests.RequestException as request_error:
        print(f"Ошибка при получении данных с сервера ЦБ РФ: {request_error}")
    except ET.ParseError as parse_error:
        print(f"Ошибка при парсинге XML: {parse_error}")
    except Exception as e_other:
        print(f"Иная ошибка: {e_other}")


def get_latest_cbr_news(start_date: datetime) -> list[dict[str, str]]:
    """
    Загружает новости и события с сайта ЦБ РФ от текущей даты до start_date.

    Args:
        start_date: Дата, до которой нужно загрузить новости (включительно)

    Returns:
        Список словарей с ключами: title, link, meta, pub_date
    """
    base_url = "https://www.cbr.ru/FPEventAndPress/"

    # Параметры запроса (все, кроме page, остаются неизменными)
    params = {
        "page": 0,
        "IsEng": "false",
        "type": 100,
        "pagesize": 10,
        "_": int(time.time() * 1000)  # Текущий timestamp в миллисекундах
    }

    collected_news_list = []
    current_page_number = 0
    should_continue_pagination = True

    while should_continue_pagination:
        params["page"] = current_page_number

        try:
            logger.info(f"Отправка запроса на {base_url} с параметрами {params}")
            response = requests.get(base_url, params=params, timeout=10)
            response.raise_for_status()

            json_data = response.json()

            # Проверяем, есть ли данные на странице
            if not json_data or len(json_data) == 0:
                break

            for news_item in json_data:
                # Парсим дату публикации
                publication_datetime = datetime.fromisoformat(news_item["DT"].replace("Z", "+00:00"))
                
                
                # Если дата новости старше start_date, прекращаем сбор
                if publication_datetime.date() < start_date.date():
                    should_continue_pagination = False
                    break

                # Формируем ссылку на новость
                article_id = news_item.get("doc_htm", "")
                if article_id.endswith(".htm"):
                    article_link = f"https://cbr.ru/press/pr/?file={article_id}"
                else:
                    article_link = f"https://cbr.ru/press/event/?id={article_id}"

                info_string = ""
                try:
                    news_response = requests.get(url=article_link)

                    if news_response.status_code == 200:
                        news_tree = html.fromstring(news_response.text)
                        paragraphs = news_tree.xpath('//div[@class="landing-text"]/p')

                        for p in paragraphs:
                            info_string += " " + drop_uwanted_symbols(p.text_content())
                            
                except requests.RequestException as request_error:
                    logger.error(f"Запрос на {article_link} не удался: {request_error}")
                except (AttributeError, TypeError) as parsing_error:
                    logger.error(f"Ошибка при парсинге HTML для {article_link}: {parsing_error}")
                except Exception as other_error:
                    logger.error(f"Неожиданная ошибка при обработке {article_link}: {other_error}")

                # Добавляем в результат
                collected_news_list.append({
                    "id": article_id,
                    "title": drop_uwanted_symbols(news_item.get("name_doc", "")),
                    "link": article_link,
                    "meta": info_string,
                    "pub_date": publication_datetime.strftime("%Y-%m-%d")
                })

                time.sleep(0.3)

            # Переходим к следующей странице
            current_page_number += 1

            # Небольшая задержка, чтобы не перегружать сервер
            time.sleep(0.3)

        except requests.RequestException as request_error:
            print(f"Ошибка при запросе страницы {current_page_number}: {request_error}")
            break
        except (KeyError, ValueError) as parsing_error:
            print(f"Ошибка при обработке данных на странице {current_page_number}: {parsing_error}")
            break

    return collected_news_list
