import logging
from pathlib import Path
import inspect

def setup_logging(log_file_path: str = None, level="INFO", logger_name: str = None):
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
    if log_file_path is not None:
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

    # Добавляем обработчик для консоли (если нужен вывод при log_file_path=None)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, level))
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Отключаем распространение логов к корневому логгеру
    logger.propagate = False

    return logger


def drop_nbsp(text: str) -> str:
    """
    Функция для удаления неразрывных пробелов из текста
    
    :param text (str): текст для очистки от неразрывных пробелов
    :return (str): текст после очистки
    """
    return text.replace('\xa0', ' ')
