import requests
import locale
import logging
import time
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

from utils import setup_logging


# Настройка логирования
logger =  logging.getLogger(__name__)
setup_logging(log_file_path="logs/ach_handler.log", level="INFO")

def get_ach_latest_docs(start_date: datetime) -> list[dict]:
    """
    Получает список последних проверок со сайта Счетной палаты (ach.gov.ru),
    фильтруя их по дате начала.

    Args:
        start_date: Объект datetime, определяющий самую раннюю дату для
                    включения документов в результат.

    Returns:
        Список словарей, где каждый словарь представляет собой информацию
        о проверке, включая заголовок, метаданные, ссылку на отчет, дату и т.д.
        Возвращает пустой список в случае ошибки.
    """

    # Устанавливаем русскую локаль для корректного разбора названий месяцев
    try:
        locale.setlocale(locale.LC_TIME, 'ru_RU.UTF-8')
    except locale.Error:
        logger.error("Локаль 'ru_RU.UTF-8' не найдена. Установите ее в вашей системе.")
        logger.warning("Попытка продолжить с системной локалью, но разбор дат может быть некорректным.")

    # Параметры запроса, как вы и указали
    api_url = "https://ach.gov.ru/api/v1/controls/list?count=10&page="
    request_headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
        'Cookie': 'i18n_redirected=ru; isDisablity=false; lang=ru',
        'Host': 'ach.gov.ru',
        'Referer': 'https://ach.gov.ru/checks',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
    }

    processed_documents_list = []
    is_enough = False
    ach_checks_list = []
    curr_page = 1
    try:

        logger.info(f"Отправка запроса на URL: {api_url}{curr_page}")
        while not is_enough:

            response = requests.get(api_url + str(curr_page), headers=request_headers, timeout=15)
            response.raise_for_status()  # Вызовет исключение для кодов ответа 4xx/5xx
            curr_cheks_list = response.json()['result']['items']
            ach_checks_list.extend(curr_cheks_list)
            logger.info(f"Получен ответ с длиной массива: {len(curr_cheks_list)}")
            if datetime.strptime(curr_cheks_list[-1]['DATE_CREATE'], "%d %B %Y") < start_date:
                is_enough = True
            else:
                curr_page += 1
            time.sleep(1.2)
                    

    except requests.exceptions.RequestException as req_e:
        logger.error(f"Ошибка сетевого запроса: {req_e}")
    except ValueError:
        logger.error("Ошибка декодирования JSON. Получен невалидный ответ от сервера.")
    except KeyError or TypeError as struct_e:
        logger.error(f"Ошибка в структуре данных: {struct_e}")

    else:

        if not ach_checks_list:
            logger.warning("В ответе API не найдены элементы проверок ('items').")
            return processed_documents_list

        logger.info(f"Получено {len(ach_checks_list)} документов. Начинаю обработку...")

        for document_item in ach_checks_list:
            # Извлекаем и парсим дату
            document_date_string = document_item.get("DATE_CREATE")
            if not document_date_string:
                logger.warning(f"Пропущен документ с ID={document_item.get('ID')} из-за отсутствия даты.")
                continue

            try:
                # Преобразуем "27 января 2026" в объект datetime
                parsed_document_date = datetime.strptime(document_date_string, "%d %B %Y")
            except ValueError:
                logger.warning(
                    f"Не удалось распознать формат даты '{document_date_string}' "
                    f"для документа ID={document_item.get('ID')}. Пропускаем."
                )
                continue

            # Фильтруем документы по дате
            if parsed_document_date >= start_date:
                # Безопасно извлекаем ссылку на первый отчет
                report_files = document_item.get("FILES", {}).get("REPORT", [])
                report_link = report_files[0].get("SRC") if report_files else None

                # Очищаем PREVIEW_TEXT от HTML-тегов
                preview_html = document_item.get("PREVIEW_TEXT", "")
                cleaned_meta_text = BeautifulSoup(preview_html, "html.parser").get_text().strip()

                document_details = {
                    "id": document_item.get("ID"),
                    "title": document_item.get("NAME"),
                    "meta": cleaned_meta_text,
                    "link": report_link,
                    "pub_date": parsed_document_date.strftime("%Y-%m-%d"),
                }
                processed_documents_list.append(document_details)

        logger.info(f"Обработано и отфильтровано {len(processed_documents_list)} документов.")

    finally:
        return processed_documents_list
