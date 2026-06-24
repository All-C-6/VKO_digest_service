import io
import logging
from pathlib import Path
import inspect
import re
import pandas as pd
import logging
import requests
import pdfplumber
from io import BytesIO
import segno
import yaml
from PIL import Image

def setup_logging(log_file_path: str = None, level="INFO", logger_name: str = None, log_to: list = ["file"]):
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


logger = logging.getLogger(__name__)
setup_logging("logs/utils.log", level="INFO")


def drop_unwanted_symbols(text: str) -> str:
    """Удаляет нежелательные символы из текста."""
    text = text.replace('\xa0', ' ')   # неразрывный пробел
    text = text.replace('\u200b', '')  # zero-width space
    text = text.replace('\u00ad', '')  # мягкий перенос
    text = text.replace('\r\n', '\n')
    text = text.replace('\r', '\n')

    # Убираем множественные пробелы (но не переносы строк)
    text = re.sub(r' {2,}', ' ', text)
    return text.strip()



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
        logger.error(f"Ошибка при загрузке PDF файла: {request_error}")
        return ""

    except Exception as general_exception:
        logger.error(f"Ошибка при обработке PDF {pdf_url}: {general_exception}")
        return ""


def convert_data_to_md(data_list: list, filename: str = None) -> str:
    """
    Конвертирует список словарей в markdown-таблицу без выравнивания пробелами.

    Args:
        data_list: список словарей с одинаковыми ключами
        filename:  имя файла для сохранения (опционально, с расширением .md или без)

    Returns:
        строка с markdown-таблицей
    """
    if not data_list:
        raise ValueError("data_list не может быть пустым")

    if not all(isinstance(item, dict) for item in data_list):
        raise TypeError("Все элементы data_list должны быть словарями")

    column_names = list(data_list[0].keys())

    header_row    = "| " + " | ".join(str(column_name) for column_name in column_names) + " |"
    separator_row = "| " + " | ".join("---" for _ in column_names) + " |"

    data_rows = []
    for item in data_list:
        row_values   = (str(item.get(column_name, "")) for column_name in column_names)
        data_row     = "| " + " | ".join(row_values) + " |"
        data_rows.append(data_row)

    markdown_table = "\n".join([header_row, separator_row] + data_rows)

    if filename is not None:
        output_path = Path(filename)
        if output_path.suffix.lower() != ".md":
            output_path = output_path.with_suffix(".md")

        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(markdown_table, encoding="utf-8")
        logger.info(f"Markdown-таблица сохранена в файл: {output_path.resolve()}")

    return markdown_table


def save_yaml(filename: str, yaml_string_data: str):
    """
    Сохранение YAML данных в файл

    Args:
        filename: путь до файла (с расширением либо без)
        yaml_string_data: данные, что нужно записать
    """
    output_path = Path(filename)
    if output_path.suffix.lower() not in (".yaml", ".yml"):
        output_path = output_path.with_suffix(".yaml")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(yaml_string_data, encoding="utf-8")
    logger.info(f"YAML сохранён в файл: {output_path.resolve()}")


def convert_data_to_yaml(data_list: list, filename: str = None) -> str:
    """
    Конвертирует список словарей в YAML-строку.

    Args:
        data_list: список словарей с одинаковыми ключами
        filename:  имя файла для сохранения (опционально, с расширением .yaml или без)

    Returns:
        строка в формате YAML
    """
    if not data_list:
        raise ValueError("data_list не может быть пустым")

    if not all(isinstance(item, dict) for item in data_list):
        raise TypeError("Все элементы data_list должны быть словарями")

    yaml_string = yaml.dump(
        data_list,
        allow_unicode=True,       # корректный вывод кириллицы без экранирования
        default_flow_style=False, # развёрнутый многострочный формат
        sort_keys=False,          # сохранить порядок ключей как в исходнике
        indent=2,                 # стандартный отступ
    )

    if filename is not None:
        save_yaml(filename, yaml_string)

    return yaml_string


def get_elements_from_yaml(attr_name: str, yaml_file_path: str) -> dict:
    """
    Находит все значения по имени атрибута в YAML файле.

    Args:
        attr_name: имя атрибута для поиска
        yaml_file_path: путь до YAML файла с данными

    Returns:
        Словарь с ключами вида "attr_name_1", "attr_name_2" и их значениями
    """

    def collect_values_recursively(node, collected_values: list) -> None:
        """Рекурсивно обходит YAML-структуру и собирает значения по имени атрибута."""
        if isinstance(node, dict):
            for key, value in node.items():
                if key == attr_name:
                    collected_values.append(value)
                else:
                    collect_values_recursively(value, collected_values)
        elif isinstance(node, list):
            for item in node:
                collect_values_recursively(item, collected_values)

    with open(yaml_file_path, "r", encoding="utf-8") as yaml_file:
        yaml_content = yaml.safe_load(yaml_file)

    collected_values = []
    collect_values_recursively(yaml_content, collected_values)

    result_dict = {
        f"{attr_name}_{index}": value
        for index, value in enumerate(collected_values, start=1)
    }

    return result_dict


def generate_qr_code(
    content: str,
    output_file_path: str,
    scale: int = 10,
    dark_color: str = "#0A5B5F",
    light_color: str = "#FFFFFF",
) -> None:
    """
    Генерирует QR-код тёмно-синего цвета и сохраняет его в формате PNG.

    Args:
        content: содержимое QR-кода (ссылка)
        output_file_path: путь для сохранения QR-кода
        scale: масштаб изображения
        dark_color: цвет тёмной области кода
        light_color: цвет светлой области кода

    Returns:
        None, сохраняет QR-код в файл
    """
    qr_code = segno.make(content, error="H")

    png_buffer = io.BytesIO()
    qr_code.save(
        png_buffer,
        kind="png",
        scale=scale,
        dark=dark_color,
        light=light_color,
    )
    png_buffer.seek(0)

    qr_image = Image.open(png_buffer)
    qr_image.save(output_file_path, format="PNG")

    print(f"QR-код сохранён в: {output_file_path}")
