from telethon import TelegramClient
from telethon.tl.types import Message
import os
from datetime import datetime
import asyncio
import json

async def get_last_posts_from_central_bank_channel(
    number_of_last_posts: int,
    api_id: int = None,
    api_hash: str = None,
    session_name: str = 'central_bank_reader_session'
) -> list[dict]:
    """
    Получает последние N постов из телеграм канала Центробанка России.

    Args:
        number_of_last_posts: Количество последних постов для получения
        api_id: ID приложения Telegram API (если None, берется из переменной окружения TELEGRAM_API_ID)
        api_hash: Hash приложения Telegram API (если None, берется из переменной окружения TELEGRAM_API_HASH)
        session_name: Имя файла сессии для сохранения авторизации

    Returns:
        Список словарей с информацией о постах. Каждый словарь содержит:
        - post_id: ID поста
        - date: Дата публикации поста
        - text: Текст поста
        - views: Количество просмотров
        - forwards: Количество пересылок
        - has_media: Наличие медиа-контента
        - media_type: Тип медиа (photo, video, document и т.д.)
        - link: Прямая ссылка на пост
    """
    # Получаем credentials из параметров или переменных окружения
    if api_id is None:
        api_id = os.getenv('TELEGRAM_API_ID')
    if api_hash is None:
        api_hash = os.getenv('TELEGRAM_API_HASH')

    if not api_id or not api_hash:
        raise ValueError(
            "Необходимо предоставить api_id и api_hash либо через параметры, "
            "либо через переменные окружения TELEGRAM_API_ID и TELEGRAM_API_HASH"
        )

    # Имя канала (можно использовать username без @)
    channel_username = 'centralbank_russia'

    collected_posts = []

    # Создаем клиент и подключаемся
    async with TelegramClient(session_name, api_id, api_hash) as telegram_client:
        try:
            # Получаем информацию о канале
            channel_entity = await telegram_client.get_entity(channel_username)

            # Получаем последние посты
            async for message in telegram_client.iter_messages(
                channel_entity, 
                limit=number_of_last_posts
            ):
                if isinstance(message, Message):
                    # Определяем тип медиа
                    media_type = None
                    has_media = False

                    if message.media:
                        has_media = True
                        if message.photo:
                            media_type = 'photo'
                        elif message.video:
                            media_type = 'video'
                        elif message.document:
                            media_type = 'document'
                        elif message.audio:
                            media_type = 'audio'
                        elif message.voice:
                            media_type = 'voice'
                        elif message.sticker:
                            media_type = 'sticker'
                        elif message.poll:
                            media_type = 'poll'
                        else:
                            media_type = 'other'

                    # Формируем прямую ссылку на пост
                    post_link = f"https://t.me/{channel_username}/{message.id}"

                    post_data = {
                        'post_id': message.id,
                        'date': message.date,
                        'text': message.text or '',
                        'views': message.views or 0,
                        'forwards': message.forwards or 0,
                        'has_media': has_media,
                        'media_type': media_type,
                        'link': post_link
                    }

                    collected_posts.append(post_data)

            return collected_posts

        except Exception as error:
            raise Exception(f"Ошибка при получении постов из канала: {str(error)}")


# Пример синхронной обёртки для удобства использования
def get_last_posts_from_central_bank_channel_sync(
    number_of_last_posts: int,
    api_id: int = None,
    api_hash: str = None
) -> list[dict]:
    """
    Синхронная версия функции получения постов.

    Args:
        number_of_last_posts: Количество последних постов для получения
        api_id: ID приложения Telegram API
        api_hash: Hash приложения Telegram API

    Returns:
        Список словарей с информацией о постах
    """

    return asyncio.run(
        get_last_posts_from_central_bank_channel(
            number_of_last_posts=number_of_last_posts,
            api_id=api_id,
            api_hash=api_hash
        )
    )
