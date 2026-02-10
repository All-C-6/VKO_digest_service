import requests
import locale
import logging
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
        logging.error("Локаль 'ru_RU.UTF-8' не найдена. Установите ее в вашей системе.")
        logging.warning("Попытка продолжить с системной локалью, но разбор дат может быть некорректным.")

    # Параметры запроса, как вы и указали
    api_url = "https://ach.gov.ru/api/v1/controls/list?count=100&page=1" # Увеличим count для большей выборки
    request_headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
        'Cookie': 'i18n_redirected=ru; isDisablity=false; lang=ru', # Оставил только необходимые cookie
        'Host': 'ach.gov.ru',
        'Referer': 'https://ach.gov.ru/checks',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
    }

    processed_documents_list = []

    try:
        logging.info(f"Отправка запроса на URL: {api_url}")
        response = requests.get(api_url, headers=request_headers, timeout=15)
        response.raise_for_status()  # Вызовет исключение для кодов ответа 4xx/5xx

        raw_data = response.json()

    except requests.exceptions.RequestException as e:
        logging.error(f"Ошибка сетевого запроса: {e}")
        return processed_documents_list
    except ValueError:
        logging.error("Ошибка декодирования JSON. Получен невалидный ответ от сервера.")
        return processed_documents_list

    # Используем .get() для безопасного доступа к данным
    audit_items = raw_data.get("result", {}).get("items", [])
    if not audit_items:
        logging.warning("В ответе API не найдены элементы проверок ('items').")
        return processed_documents_list

    logging.info(f"Получено {len(audit_items)} документов. Начинаю обработку...")

    for document_item in audit_items:
        # Извлекаем и парсим дату
        document_date_string = document_item.get("DATE_CREATE")
        if not document_date_string:
            logging.warning(f"Пропущен документ с ID={document_item.get('ID')} из-за отсутствия даты.")
            continue

        try:
            # Преобразуем "27 января 2026" в объект datetime
            parsed_document_date = datetime.strptime(document_date_string, "%d %B %Y")
        except ValueError:
            logging.warning(
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
                "pub_date": parsed_document_date,
            }
            processed_documents_list.append(document_details)

    logging.info(f"Обработано и отфильтровано {len(processed_documents_list)} документов.")

    return processed_documents_list

# --- Пример использования функции ---
if __name__ == "__main__":
    # Устанавливаем дату, с которой мы хотим получать документы
    # Например, за последние 30 дней от сегодняшней даты (6 февраля 2026)
    thirty_days_ago = datetime.now() - timedelta(days=30)

    print(f"Ищем документы, опубликованные с {thirty_days_ago.strftime('%d.%m.%Y')}")
    print("-" * 30)

    # Вызываем основную функцию
    latest_documents = get_ach_latest_docs(start_date=thirty_days_ago)

    if latest_documents:
        logging.info(f"Найдено {len(latest_documents)} новых документов.")

        # Выведем информацию о первом найденном документе для примера
        first_document = latest_documents[0]

        print(f"Имеются поля: {first_document.keys()}")
    else:
        logging.info("Новых документов за указанный период не найдено.")