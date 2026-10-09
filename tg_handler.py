import asyncio
from datetime import datetime, timezone
import re
from typing import Any, Dict, List
from telethon import TelegramClient
from telethon.tl.types import (
    MessageEntityTextUrl,
    MessageEntityUrl,
)

from utils import setup_logging

URL_REGEX = re.compile(r'https?://[^\s<>"]+|www\.[^\s<>"]+')
logger = setup_logging(log_file_path="logs/tg_handler.log", level="INFO")

def extract_links(message) -> List[str]:
    """Извлекает все типы ссылок: открытые URL и зашитые в текст гиперссылки."""
    links = set()
    text = message.text or ""

    # Извлечение из разметки Telegram (entities)
    if message.entities:
        for entity in message.entities:
            if isinstance(entity, MessageEntityTextUrl):
                # Ссылка вида [текст](https://...)
                links.add(entity.url)
            elif isinstance(entity, MessageEntityUrl):
                # Обычная ссылка в тексте
                offset = entity.offset
                length = entity.length
                url = text[offset : offset + length]
                if not url.startswith(("http://", "https://")):
                    url = "https://" + url
                links.add(url)

    # Резервный поиск через regex
    for match in URL_REGEX.findall(text):
        url = match if match.startswith(("http://", "https://")) else "https://" + match
        links.add(url)

    return list(links)


async def get_channel_posts_by_dates(
    client: TelegramClient,
    channel: str,
    start_date: datetime,
    end_date: datetime,
) -> List[Dict[str, Any]]:
    """
    Возвращает список постов канала с полным текстом, параметрами
    и отдельным списком извлеченных ссылок.
    """
    if start_date.tzinfo is None:
        start_date = start_date.replace(tzinfo=timezone.utc)
    if end_date.tzinfo is None:
        end_date = end_date.replace(tzinfo=timezone.utc)

    posts = []

    async for message in client.iter_messages(channel, offset_date=end_date):
        if not message or message.action:
            continue

        if message.date > end_date:
            continue

        if message.date < start_date:
            break

        post_data = {
            "id": message.id,
            "link": f"https://t.me/{channel}/{message.id}", 
            "date": message.date.isoformat(),
            "text": message.text or "",  # Полный текст сообщения
            "views": message.views,
            "forwards": message.forwards,
            "replies": message.replies.replies if message.replies else 0,
            "has_media": bool(message.media),
            "media_type": type(message.media).__name__ if message.media else None,
            "attached_links": extract_links(message),  # Вложенный список ссылок
        }
        posts.append(format_tg_post(post_data))



    posts.reverse()
    return posts


def remove_markdown_links(text: str) -> str:
    # Шаблон находит [анкор](url)
    # \[([^\]]+)\] захватывает текст внутри скобок
    # \([^\)]+\) сопоставляется со ссылкой внутри круглых скобок
    pattern = r'\[([^\]]+)\]\([^\)]+\)'
    
    # Заменяем всю конструкцию на содержимое первой группы (\1)
    return re.sub(pattern, r'\1', text)


def format_tg_post(post: dict) -> dict:
    """
    document_info = {
        'id': document_guid.text.strip(),
        'title': document_title.text.strip(),
        'link': document_link.text.strip(),
        'meta': document_description.text.strip() if document_description is not None else '',
        'pub_date': parsed_publication_datetime.isoformat()
    }
    """
    text = post['text']
    formatted_text = remove_markdown_links(text)
    formatted_date = datetime.fromisoformat(post["date"]).strftime("%Y-%m-%d")
    title = ""
    if "\n" in text:
        title = text.split("\n")[0]
    elif re.search(". [А-Я]", text):
        title = re.split(". [А-Я]")[0]
    
    return {"id": str(post["id"]), "title": title, "link": post["link"], "meta": formatted_text, "pub_date": formatted_date}




def get_latest_cbr_posts(start_date, end_date, CB_USERNAME, API_ID, API_HASH):

    try:
        # Инициализируем клиент
        client = TelegramClient("session_name", API_ID, API_HASH)
        client.start()
        # Запускаем корутину внутри loop клиента
        posts = client.loop.run_until_complete(
            get_channel_posts_by_dates(client, CB_USERNAME, start_date, end_date)
        )
        """
        document_info = {
            'id': document_guid.text.strip(),
            'title': document_title.text.strip(),
            'link': document_link.text.strip(),
            'meta': document_description.text.strip() if document_description is not None else '',
            'pub_date': parsed_publication_datetime.isoformat()
        }
        """
        logger.debug(f"Получено постов: {len(posts)}")

    except Exception as e:
        logger.error(f"Ошибка обращения к Telegram: {e}")
        return []

    else:
        return posts

    finally:
        # Обязательно корректно закрываем сессию
        client.disconnect()