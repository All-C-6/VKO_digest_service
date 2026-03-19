import re

import yaml
from pathlib import Path
from openai import OpenAI


def load_deepseek_settings(settings_file_path: str = "settings.yaml") -> dict:
    """
    Загружает конфигурацию DeepSeek из YAML-файла.

    Args:
        settings_file_path: Путь к файлу настроек.

    Returns:
        Словарь с настройками секции 'deepseek'.
    """
    yaml_file_path = Path(settings_file_path)

    if not yaml_file_path.exists():
        raise FileNotFoundError(f"Файл настроек не найден: {yaml_file_path.resolve()}")

    with yaml_file_path.open(encoding="utf-8") as yaml_file:
        all_settings = yaml.safe_load(yaml_file)

    if "deepseek" not in all_settings:
        raise KeyError("В файле настроек отсутствует секция 'deepseek'")

    return all_settings["deepseek"]


def get_list_summary(string_with_data: str, prompt: str, settings_file_path: str = "settings.yaml") -> str:
    """
    Отправляет запрос к DeepSeek API и возвращает саммари по переданным данным.

    DeepSeek совместим с OpenAI SDK, поэтому используется openai.OpenAI клиент
    с переопределённым base_url.

    Args:
        string_with_data: Строка с данными, которые нужно суммаризировать.
        prompt: Инструкция или вопрос к модели относительно данных.
        settings_file_path: Путь к YAML-файлу с настройками (по умолчанию "settings.yaml").

    Returns:
        Текстовый ответ языковой модели.

    Raises:
        FileNotFoundError: Если файл настроек не найден.
        KeyError: Если в файле настроек отсутствует секция 'deepseek'.
        openai.APIError: При ошибке на стороне API.
    """
    deepseek_settings = load_deepseek_settings(settings_file_path)

    deepseek_client = OpenAI(
        api_key=deepseek_settings["api_key"],
        base_url=deepseek_settings["base_url"],
    )

    system_message = {
        "role": "system",
        "content": deepseek_settings["system_prompt"],
    }

    user_message_content = f"{prompt}\n\nДанные:\n{string_with_data}"
    user_message = {
        "role": "user",
        "content": user_message_content,
    }

    chat_completion_response = deepseek_client.chat.completions.create(
        model=deepseek_settings["model"],
        messages=[system_message, user_message],
        temperature=deepseek_settings["temperature"],
        max_tokens=deepseek_settings["max_tokens"],
    )

    llm_response_text = chat_completion_response.choices[0].message.content
    return llm_response_text


def extract_items_from_llm_answer(answer: str) -> set[str]:
    """
    Извлекает идентификаторы из ответа LLM.

    Идентификатор — это последовательность цифр и латинских букв (a-z, A-Z, 0-9),
    которой предшествует от 0 до 10 не-цифровых символов от начала строки.
    Идентификатор заканчивается при появлении любого символа, не являющегося
    цифрой или латинской буквой.

    Args:
        answer: Текст ответа LLM, в котором нужно найти идентификаторы.

    Returns:
        Множество найденных идентификаторов.
    """
    pattern = re.compile(
        r"^[^\d]{0,10}"   # от 0 до 10 не-цифровых символов от начала строки
        r"([A-Za-z0-9_-]+)" # идентификатор: цифры и латинские буквы
        r"(?:[^A-Za-z0-9]|$)",  # конец идентификатора: не цифра/не лат. буква или конец строки
        re.MULTILINE
    )

    found_identifiers = {match.group(1) for match in pattern.finditer(answer)}
    return found_identifiers


def filter_valid_news_and_docs(data: list[dict], filtered_IDs: set[str]) -> list[dict]:
    """
    Фильтрует данные согласно выделенным из ответа LLM идентификаторам

    Args:
        data: данные из парсеров (полный список словарей с интересующим нас ключом 'id')
        filtered_IDs: набор идентификаторов, извлеченных из ответа LLM
    
    Returns:
        Список новостей и документов, для которых совпали идентификаторы
    
    """

    # простое итерирование и сравнение

    valid_data = [item for item in data if item['id'] in filtered_IDs]

    return valid_data