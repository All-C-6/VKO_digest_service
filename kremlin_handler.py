import requests
from lxml import html, etree
from typing import Optional
import logging
from bs4 import BeautifulSoup
from datetime import datetime
import time

from utils import setup_logging


# Настройка логирования
logger = logging.getLogger(__name__)
setup_logging(log_file_path="logs/kremlin_handler.log", level="INFO")

def get_webpage_as_xml_tree(
    url: str,
    cookie_sid: Optional[str] = None,
    timeout_seconds: int = 30,
    verify_ssl: bool = True
) -> Optional[etree._Element]:
    """
    Получает веб-страницу по указанному URL и возвращает её в виде XML-дерева через lxml.

    Args:
        url: URL адрес страницы для получения
        cookie_sid: Значение cookie 'sid' (опционально)
        timeout_seconds: Таймаут запроса в секундах
        verify_ssl: Проверять ли SSL сертификат

    Returns:
        XML-дерево (lxml.etree._Element) или None в случае ошибки

    Example:
        >>> tree = get_webpage_as_xml_tree('http://kremlin.ru/events/president/news/78823')
        >>> if tree is not None:
        >>>     title = tree.xpath('//title/text()')
        >>>     print(title)
    """

    # Формируем заголовки запроса на основе предоставленных данных
    request_headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'ru,en;q=0.9',
        'Connection': 'keep-alive',
        'Host': 'kremlin.ru',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 YaBrowser/25.8.0.0 Safari/537.36'
    }

    # Формируем cookies если предоставлен sid
    request_cookies = {}
    if cookie_sid:
        request_cookies['sid'] = cookie_sid

    try:
        logger.info(f"Отправка GET запроса к URL: {url}")

        # Выполняем GET запрос
        response = requests.get(
            url,
            headers=request_headers,
            cookies=request_cookies if request_cookies else None,
            timeout=timeout_seconds,
            verify=verify_ssl
        )

        # Проверяем статус ответа
        response.raise_for_status()

        logger.info(f"Получен ответ со статусом: {response.status_code}")
        logger.info(f"Content-Type: {response.headers.get('Content-Type', 'не указан')}")
        logger.info(f"Content-Encoding: {response.headers.get('Content-Encoding', 'не указан')}")

        # Парсим HTML в XML-дерево
        # Используем response.content для корректной обработки кодировки
        xml_tree = html.fromstring(response.content)

        logger.info("HTML успешно распарсен в XML-дерево")

        return xml_tree

    except requests.exceptions.Timeout:
        logger.error(f"Превышен таймаут ожидания ответа ({timeout_seconds}с) для URL: {url}")
        return None

    except requests.exceptions.ConnectionError as connection_error:
        logger.error(f"Ошибка соединения с {url}: {connection_error}")
        return None

    except requests.exceptions.HTTPError as http_error:
        logger.error(f"HTTP ошибка для {url}: {http_error}")
        logger.error(f"Статус код: {response.status_code}")
        return None

    except etree.ParserError as parser_error:
        logger.error(f"Ошибка парсинга HTML в XML-дерево: {parser_error}")
        return None

    except Exception as unexpected_error:
        logger.error(f"Неожиданная ошибка при обработке {url}: {unexpected_error}")
        return None


def extract_article_text_with_options(
    xml_tree: etree._Element,
    article_class_name: str = "read__in hentry h-entry",
    paragraph_separator: str = " ",
    strip_extra_whitespace: bool = True,
    include_paragraph_numbers: bool = False
) -> Optional[str]:
    """
    Расширенная версия функции извлечения текста с дополнительными опциями.

    Args:
        xml_tree: XML-дерево для обработки
        article_class_name: Класс article элемента
        paragraph_separator: Разделитель между параграфами
        strip_extra_whitespace: Удалять ли лишние пробелы
        include_paragraph_numbers: Добавлять ли номера параграфов

    Returns:
        Объединенный текст статьи с примененными опциями
    """

    try:
        logger.info(f"Расширенное извлечение текста из <article class='{article_class_name}'>")

        # Поиск article элемента
        article_elements = xml_tree.xpath(f"//article[contains(@class, 'read__in')]")

        if not article_elements:
            logger.warning(f"Элемент <article> не найден")
            return None

        article_element = article_elements[0]
        paragraph_elements = article_element.xpath('.//p')

        if not paragraph_elements:
            logger.warning("Параграфы <p> не найдены")
            return None

        logger.info(f"Обработка {len(paragraph_elements)} параграфов")

        all_paragraphs_text_list = []

        for paragraph_index, paragraph_element in enumerate(paragraph_elements, start=1):
            # Извлекаем текст только из <p> и вложенных <a>
            text_from_p_and_a = paragraph_element.xpath('.//text()[parent::p or parent::a]')
            paragraph_filtered_text = ''.join(text_from_p_and_a)

            # Применяем опцию очистки пробелов
            if strip_extra_whitespace:
                paragraph_cleaned_text = ' '.join(paragraph_filtered_text.split())
            else:
                paragraph_cleaned_text = paragraph_filtered_text

            if paragraph_cleaned_text:
                # Применяем опцию нумерации параграфов
                if include_paragraph_numbers:
                    paragraph_formatted_text = f"[{paragraph_index}] {paragraph_cleaned_text}"
                else:
                    paragraph_formatted_text = paragraph_cleaned_text

                all_paragraphs_text_list.append(paragraph_formatted_text)

        # Объединяем с указанным разделителем
        article_combined_text = paragraph_separator.join(all_paragraphs_text_list)

        logger.info(f"Извлечено {len(all_paragraphs_text_list)} параграфов, "
                   f"общая длина: {len(article_combined_text)} символов")

        return article_combined_text if article_combined_text else None

    except Exception as unexpected_error:
        logger.error(f"Ошибка при расширенном извлечении текста: {unexpected_error}")
        return None


def get_latest_kremlin_docs(start_date: datetime, end_date=datetime.today(), delay_between_requests: float = 1.0) -> list[dict[str, str]]:
    """
    Загружает список нормативно-правовых актов с сайта kremlin.ru за указанный период.

    Args:
        start_date (datetime): Дата начала периода, объект datetime
        end_date (datetime): Дата окончания периода, объект datetime, по-умолчанию сегодня
        delay_between_requests (float): Задержка между запросами в секундах

    Returns:
        List[Dict[str, str]]: Список словарей с информацией о документах.
                             Каждый словарь содержит поля:
                             - 'title': заголовок документа
                             - 'meta': дополнительная информация (например, "О Храмове О.В.")
                             - 'date': дата публикации
                             - 'datetime': дата в формате datetime
                             - 'url': полный URL документа
    """

    # Настройка логирования
    logger = logging.getLogger(__name__)

    base_url = "http://kremlin.ru/acts/bank/search"
    kremlin_base_url = "http://kremlin.ru"
    documents_list = []
    current_page = 1

    # Заголовки для имитации браузера
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'ru-RU,ru;q=0.9,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }

    while True:
        try:
            # Формирование URL для текущей страницы
            params = {
                'date_since': start_date.strftime("%d.%m.%y"),
                'date_till': end_date.strftime("%d.%m.%y")
            }

            if current_page > 1:
                params['page'] = current_page

            logger.info(f"Загрузка страницы {current_page} за период {start_date} - {end_date}")

            # Выполнение HTTP запроса
            response = requests.get(base_url, params=params, headers=headers, timeout=30)
            response.raise_for_status()
            response.encoding = 'utf-8'

            # Парсинг HTML содержимого
            soup = BeautifulSoup(response.text, 'html.parser')

            # Поиск контейнера с событиями
            events_container = soup.find('div', class_='events')

            if not events_container:
                logger.warning(f"Контейнер с классом 'events' не найден на странице {current_page}")
                break

            # Поиск документов на текущей странице
            document_entries = events_container.find_all('div', class_=['hentry', 'hentry_event', 'hentry_doc'])

            if not document_entries:
                logger.info(f"Документы не найдены на странице {current_page}. Завершение поиска.")
                break

            # Обработка найденных документов
            for document_entry in document_entries:
                try:
                    document_info = parse_single_document_entry(document_entry, kremlin_base_url)
                    if document_info:
                        documents_list.append(document_info)
                        logger.debug(f"Добавлен документ: {document_info['title'][:50]}...")

                except Exception as parsing_error:
                    logger.error(f"Ошибка при парсинге документа: {parsing_error}")
                    continue

            logger.info(f"На странице {current_page} найдено {len(document_entries)} документов")

            # Переход к следующей странице
            current_page += 1

            # Задержка между запросами для избежания блокировки
            if delay_between_requests > 0:
                time.sleep(delay_between_requests)

        except requests.RequestException as request_error:
            logger.error(f"Ошибка HTTP запроса на странице {current_page}: {request_error}")
            break
        except Exception as unexpected_error:
            logger.error(f"Неожиданная ошибка на странице {current_page}: {unexpected_error}")
            break

    logger.info(f"Всего загружено {len(documents_list)} документов за период {start_date} - {end_date}")
    return documents_list


def parse_single_document_entry(document_entry, kremlin_base_url: str) -> Optional[dict[str, str]]:
    """
    Парсит информацию об отдельном документе из HTML элемента.

    Args:
        document_entry: HTML элемент с информацией о документе
        kremlin_base_url (str): Базовый URL сайта кремля

    Returns:
        Optional[Dict[str, str]]: Словарь с информацией о документе или None в случае ошибки
    """

    # Поиск заголовка и ссылки
    title_element = document_entry.find('h3', class_='hentry__title')
    if not title_element:
        return None

    link_element = title_element.find('a')
    if not link_element or not link_element.get('href'):
        return None

    # Извлечение основного текста ссылки (заголовка документа)
    document_title = link_element.get_text(strip=True)

    # Поиск дополнительной мета-информации
    meta_acts_element = link_element.find('span', class_='hentry__meta_acts')
    document_meta = drop_nbsp(meta_acts_element.get_text(strip=True)) if meta_acts_element else ""

    # Поиск даты публикации
    time_element = link_element.find('time')
    document_date = drop_nbsp(time_element.get_text(strip=True)) if time_element else ""
    document_datetime = time_element.get('datetime') if time_element else ""

    # Формирование полного URL
    document_href = link_element.get('href')
    full_document_url = f"{kremlin_base_url}{document_href}"

    # Очистка заголовка от мета-информации и даты
    clean_document_title = document_title
    if document_meta:
        clean_document_title = drop_nbsp(clean_document_title.replace(document_meta, '').strip())
    if document_date:
        clean_document_title = drop_nbsp(clean_document_title.replace(document_date, '').strip())

    return {
        'title': clean_document_title,
        'meta': document_meta,
        'date': document_date,
        'datetime': document_datetime,
        'link': full_document_url
    }


def drop_nbsp(text: str) -> str:
    """
    Функция для удаления неразрывных пробелов из текста
    
    :param text (str): текст для очистки от неразрывных пробелов
    :return (str): текст после очистки
    """
    return text.replace('\xa0', ' ')
