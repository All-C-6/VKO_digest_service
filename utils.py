import logging
from pathlib import Path
import inspect
import pandas as pd
import logging
import requests
import pdfplumber
from io import BytesIO


def setup_logging(log_to: list = ["file"], log_file_path: str = None, level="INFO", logger_name: str = None):
    """
    Установка логгирования для конкретного модуля
    Создает отдельный логгер с собственным файлом, не влияя на другие модули

    Args:
        log_file_path: Путь к файлу логов.

                      None - логи только в консоль, файл не создается

                      "default" - автоматический путь logs/{logger_name}.log

                      str - явный путь к файлу логов
        level: Уровень логирования
        logger_name: Имя логгера (по умолчанию - имя вызывающего модуля)
    """
    # Определение имени логгера (по имени модуля вызывающего файла)
    if logger_name is None:
        caller_name = inspect.stack()[1].filename
        logger_name = Path(caller_name).stem

    if level not in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
        level = "DEBUG"

    # Получаем или создаем логгер для конкретного модуля
    logger = logging.getLogger(logger_name)
    logger.setLevel(getattr(logging, level))

    # Очищаем существующие обработчики (чтобы избежать дублирования)
    logger.handlers.clear()

    # Создаем форматтер
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Если log_file_path не None, создаем обработчик для файла
    if log_file_path is not None and "file" in log_to:
        # Определение пути к файлу логов
        if log_file_path == "default":
            log_file_path = f'logs/{logger_name}.log'

        script_dir = Path(__file__).parent.absolute()
        full_log_file_path = Path(f"{script_dir}/{log_file_path}")
        full_log_file_path.parent.mkdir(parents=True, exist_ok=True)

        # Создаем обработчик для файла
        file_handler = logging.FileHandler(
            full_log_file_path, 
            mode='a', 
            encoding='utf-8'
        )
        file_handler.setLevel(getattr(logging, level))
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    # Если задан вывод в консоль, добавляем обработчик для консоли
    if "console" in log_to:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(getattr(logging, level))
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    # Отключаем распространение логов к корневому логгеру
    logger.propagate = False

    return logger


def drop_uwanted_symbols(text: str) -> str:
    """
    Функция для удаления неразрывных пробелов из текста
    
    :param text (str): текст для очистки от неразрывных пробелов
    :return (str): текст после очистки
    """
    return text.replace('\xa0', ' ').replace('\n', ' ')


def save_list_dict_to_excel(
    data_list_of_dictionaries: list[dict[str]], 
    output_excel_file_path: str,
    sheet_name: str = "Sheet1",
    index_column_included: bool = False
) -> bool:
    """
    Сохраняет список словарей в Excel файл (.xlsx).

    Args:
        data_list_of_dictionaries: Список словарей для сохранения
        output_excel_file_path: Путь к выходному Excel файлу
        sheet_name: Название листа в Excel файле (по умолчанию "Sheet1")
        index_column_included: Включать ли индексную колонку (по умолчанию False)

    Returns:
        bool: True если сохранение прошло успешно, False в случае ошибки

    Raises:
        ValueError: Если список пуст или содержит некорректные данные
        FileNotFoundError: Если не удается создать файл по указанному пути
    """

    # Проверяем входные данные
    if not data_list_of_dictionaries:
        error_message = "Список словарей пуст - нечего сохранять"
        logging.error(error_message)
        raise ValueError(error_message)

    if not isinstance(data_list_of_dictionaries, list):
        error_message = "Входные данные должны быть списком"
        logging.error(error_message)
        raise ValueError(error_message)

    # Проверяем что все элементы являются словарями
    for index, dictionary_item in enumerate(data_list_of_dictionaries):
        if not isinstance(dictionary_item, dict):
            error_message = f"Элемент с индексом {index} не является словарем"
            logging.error(error_message)
            raise ValueError(error_message)

    try:
        # Создаем DataFrame из списка словарей
        dataframe_from_dictionaries = pd.DataFrame(data_list_of_dictionaries)

        logging.info(f"Создан DataFrame с размерностью {dataframe_from_dictionaries.shape}")
        logging.info(f"Столбцы таблицы: {list(dataframe_from_dictionaries.columns)}")

        # Сохраняем в Excel файл
        with pd.ExcelWriter(
            output_excel_file_path, 
            engine='openpyxl', 
            mode='w'
        ) as excel_writer:
            dataframe_from_dictionaries.to_excel(
                excel_writer,
                sheet_name=sheet_name,
                index=index_column_included,
                na_rep=''  # Заменяем NaN пустыми строками
            )

        logging.info(f"Данные успешно сохранены в файл: {output_excel_file_path}")
        logging.info(f"Лист: {sheet_name}, строк данных: {len(data_list_of_dictionaries)}")

        return True

    except PermissionError as permission_error:
        error_message = f"Нет прав для записи в файл {output_excel_file_path}: {permission_error}"
        logging.error(error_message)
        return False

    except FileNotFoundError as file_not_found_error:
        error_message = f"Не удается создать файл по пути {output_excel_file_path}: {file_not_found_error}"
        logging.error(error_message)
        raise FileNotFoundError(error_message)

    except Exception as unexpected_error:
        error_message = f"Неожиданная ошибка при сохранении в Excel: {unexpected_error}"
        logging.error(error_message)
        return False


def extract_pdf_full_text_advanced(pdf_url: str) -> str:
    """
    Извлекает весь текст из PDF файла по указанной ссылке с использованием pdfplumber.

    Args:
        pdf_url (str): URL ссылка на PDF файл

    Returns:
        Optional[str]: Извлеченный текст из PDF или None в случае ошибки
    """
    try:
        # Скачиваем PDF файл
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

        response = requests.get(pdf_url, headers=headers, timeout=30)
        response.raise_for_status()

        # Создаем объект BytesIO из загруженного содержимого
        pdf_bytes_io = BytesIO(response.content)

        extracted_complete_text = ""

        # Используем pdfplumber для более качественного извлечения текста
        with pdfplumber.open(pdf_bytes_io) as pdf_document:
            for page_index, current_page in enumerate(pdf_document.pages):
                page_text_content = current_page.extract_text()
                if page_text_content:
                    extracted_complete_text += page_text_content + "\n"

        return extracted_complete_text.strip()

    except requests.exceptions.RequestException as request_error:
        print(f"Ошибка при загрузке PDF файла: {request_error}")
        return ""

    except Exception as general_exception:
        print(f"Ошибка при обработке PDF {pdf_url}: {general_exception}")
        return ""