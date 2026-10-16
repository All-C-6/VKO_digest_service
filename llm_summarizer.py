import re
import yaml
from pathlib import Path
from openai import OpenAI
from logging import getLogger

from utils import setup_logging


logger = getLogger(__name__)
setup_logging(log_file_path="logs/llm_summarizer.log", level="INFO")


def get_list_summary(string_with_data: str, prompt: str, deepseek_settings: dict) -> str:
    """
    Отправляет запрос к DeepSeek API и возвращает саммари по переданным данным.

    DeepSeek совместим с OpenAI SDK, поэтому используется openai.OpenAI клиент
    с переопределённым base_url.

    Args:
        string_with_data: Строка с данными, которые нужно суммаризировать.
        prompt: Инструкция или вопрос к модели относительно данных.
        deepseek_settings: настройки DeepSeek (пути, параметры генерации)

    Returns:
        Текстовый ответ языковой модели.

    Raises:
        FileNotFoundError: Если файл настроек не найден.
        KeyError: Если в файле настроек отсутствует секция 'deepseek'.
        openai.APIError: При ошибке на стороне API.
    """

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

    if not found_identifiers:
        logger.error(f"Не удалось извлечь идентификаторы из ответа LLM. Проверьте формат вывода.")
        raise ValueError(f"Не удалось извлечь идентификаторы из ответа LLM. Проверьте формат вывода.")
    return found_identifiers


def filter_valid_news_and_docs(data: list[dict], filtered_IDs: set[str]) -> list[dict]:
    """
    Фильтрует список словарей по ключу 'id' на основании набора идентификаторов.

    Сравнение выполняется через startswith, так как идентификатор в наборе
    может быть усечённой версией полного id (без точки и суффикса после неё).
    Например, 'DOC123' из набора совпадёт с 'DOC123.pdf' в данных.

    Args:
        data: список словарей с ключом 'id'
        filtered_IDs: набор идентификаторов для отбора

    Returns:
        отфильтрованный список словарей, чей 'id' начинается
        с одного из идентификаторов из набора
    """
    def item_id_matches_any_chosen_id(item: dict) -> bool:
        item_id = item.get('id', '')
        return any(item_id.startswith(chosen_id) for chosen_id in filtered_IDs)

    return list(filter(item_id_matches_any_chosen_id, data))


def summarize_single_news(
    news_item: dict[str],
    deepseek_settings: dict,
    custom_instruction: str = ""
) -> str:
    """Генерирует краткое саммари (строго в один абзац) для одной новости/документа.

    Args:
        news_item: Словарь с полями новости ('id', 'title', 'meta', 'link' и т.д.).
        custom_instruction: Дополнительные указания для модели (опционально).
        settings_file_path: Путь к YAML-файлу с настройками (по умолчанию "settings.yaml").

    Returns:
        Саммари новости в виде связного текста длиной в один абзац.

    Raises:
        FileNotFoundError: Если файл настроек не найден.
        KeyError: Если в настройках отсутствует секция 'deepseek'.
        openai.APIError: При ошибке обращения к DeepSeek API.
    """

    deepseek_client = OpenAI(
        api_key=deepseek_settings["api_key"],
        base_url=deepseek_settings["base_url"],
    )

    system_prompt = (
        "Ты — аналитик регуляторных рисков и финансового сектора. "
        "Твоя задача — изложить суть переданного документа или новости "
        "строго в один емкий абзац (3–5 предложений). "
        "Отражай только факты, регуляторные последствия, сроки и требования. "
        "Не используй вводные конструкции (например, «В данной новости...», «Сообщается, что...»), "
        "списки, заголовки и переносы строк."
    )

    # Собираем контекст конкретной новости
    news_title = news_item.get("title", "")
    news_content = news_item.get("meta", "")
    news_date = news_item.get("pub_date", "")

    user_content_parts = [
        f"Заголовок: {news_title}",
        f"Дата публикации: {news_date}" if news_date else "",
        f"Содержание/Пресс-релиз:\n{news_content}",
    ]
    if custom_instruction:
        user_content_parts.append(f"Дополнительное требование: {custom_instruction}")

    user_message_content = "\n\n".join(part for part in user_content_parts if part)

    chat_completion_response = deepseek_client.chat.completions.create(
        model=deepseek_settings["model"],
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message_content},
        ],
        temperature=deepseek_settings.get("temperature", 0.2),
        max_tokens=deepseek_settings.get("max_tokens", 300),
    )

    summary = chat_completion_response.choices[0].message.content.strip()
    return summary