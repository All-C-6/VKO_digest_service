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


def extract_links_from_md(text: str) -> set:
    """Извлекает ссылки из Markdown-текста от LLM.

    Поддерживает:
    - Голые ссылки: - https://example.com - Описание
    - MD-ссылки: 1. [Описание](https://example.com)
    - Автоссылки: * <https://example.com> — Описание
    """
    results = set()

    # Паттерн ищет:
    # 1. Опциональный маркер списка в начале строки (-, *, +, 1.)
    # 2. Либо MD-ссылку [title](url), либо голый/угловой URL
    # 3. Опциональный разделитель (-, –, —, :) и оставшийся текст строки как описание
    pattern = re.compile(
        r"^\s*(?:[-*+]|\d+[.)])?\s*"
        r"(?:\[(?P<md_title>[^\]]+)\]\((?P<md_url>https?://[^\s)]+)\)|<?(?P<raw_url>https?://[^\s>]+)>?)"
        r"(?:\s*[-–—:]\s*(?P<desc>.+))?$",
        re.MULTILINE,
    )

    for match in pattern.finditer(text):
        url = match.group("md_url") or match.group("raw_url")
        # Очищаем случайную пунктуацию в конце голого URL (например, точку или запятую)
        url = url.rstrip(".,;:!?")

        results.add(url)

    return results


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


def summarize_single_item(
    news_item: dict[str],
    deepseek_settings: dict,
    custom_instruction: str = ""
) -> str:
    """Генерирует краткое саммари (строго в один абзац) для одной новости/документа.

    Args:
        news_item: Словарь с полями новости ('id', 'title', 'desc', 'type' и 'pub_date'.).
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
    news_content = news_item.get("desc", "")
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