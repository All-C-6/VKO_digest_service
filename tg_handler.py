import asyncio
from datetime import datetime, timezone
import re
from typing import Any, Dict, List
from telethon import TelegramClient
from telethon.tl.types import (
    MessageEntityTextUrl,
    MessageEntityUrl,
)

URL_REGEX = re.compile(r'https?://[^\s<>"]+|www\.[^\s<>"]+')


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
            "date": message.date.isoformat(),
            "text": message.text or "",  # Полный текст сообщения
            "views": message.views,
            "forwards": message.forwards,
            "replies": message.replies.replies if message.replies else 0,
            "has_media": bool(message.media),
            "media_type": type(message.media).__name__ if message.media else None,
            "links": extract_links(message),  # Вложенный список ссылок
        }
        posts.append(post_data)

    posts.reverse()
    return posts