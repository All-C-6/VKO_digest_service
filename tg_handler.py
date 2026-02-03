import datetime
import requests
import re
from telethon import events
from telethon.sessions import StringSession
from telethon.sync import TelegramClient
from telethon.tl.types import InputMediaPoll, MessageMediaPoll, MessageEntityUrl, Message
from telethon import functions, types
from telethon.errors.rpcerrorlist import MsgIdInvalidError
from typing import List, Tuple, Dict
import pandas as pd
import time
from tqdm import tqdm
import pickle
import traceback
import warnings
warnings.filterwarnings("ignore")


class Stat():

    # Id и Hash приложения Telegram
    API_ID = 34776100
    API_HASH = "7d64e4ea96b357016ae00e630f6c57ed"

    def __init__(self, CHAT_ID_in=None):

        self.CHAT_ID = CHAT_ID_in
        if CHAT_ID_in is None:
            self.CHAT_ID = 'fiztransform'
        else:
            self.CHAT_ID = CHAT_ID_in
        try:
            self.client = TelegramClient('anon', Stat.API_ID, Stat.API_HASH).start()
            self.channel = self.client.get_entity(self.CHAT_ID)
        except:
            print('No get current channel data :(')

    def _get_session_string(self):
        with TelegramClient(StringSession(), Stat.API_ID, Stat.API_HASH) as client:
                print(client.session.save())

    def _get_urls(self, message: MessageEntityUrl) -> List[str]:

        entities = message.entities
        tele_urls = list()
        if entities:
            urls = [(url.offset, url.offset + url.length) for url in entities if isinstance(url, MessageEntityUrl)]
            tele_urls = [message.message[tele_url[0]: tele_url[1]] for tele_url in urls]
        return tele_urls

    def _get_messages(self) -> List[Message]:

        messages = list()
        for message in self.client.iter_messages(self.channel):
            messages.append(message)
        return messages

    def _get_title(self, url: str) -> str:

        try:
            response = requests.get(url, verify=False)
            return re.search('(?<=<title>).+?(?=</title>)', response.text, re.DOTALL).group().strip()
        except:
            return None

    def _get_all_views(self) -> int:
        messages = self._get_messages()
        views = 0
        for message in messages:
            if message.views:
                views += message.views
        return views

    def _get_message_comments(self, message: Message) -> List[Message]:

        try:
            message_comments = list()
            offset = 0
            limit = 100
            while True:
                comments_batch = self.client(functions.messages.GetRepliesRequest(
                                                    peer=self.channel,
                                                    msg_id=message.id,
                                                    offset_id=offset,
                                                    offset_date=0,
                                                    add_offset=0,
                                                    limit=limit,
                                                    max_id=0,
                                                    min_id=0,
                                                    hash=0

                ))
                if not comments_batch.messages:
                    break
                message_comments.extend(comments_batch.messages)
                offset = comments_batch.messages[-1].id
            return message_comments
        except MsgIdInvalidError:
            return []
        
    def get_posts_data(self) -> List:
        posts_data = []
        messages = self._get_messages()
        for message in messages:
            posts_data.append((self.CHAT_ID, message.id, message.date.replace(tzinfo=None), message.message)
                )
        return posts_data

    def get_comments_data(self) -> Dict:
        comments = []
        messages = self._get_messages()
        for message in tqdm(messages):
            for comment in self._get_message_comments(message):
                comment.append((self.CHAT_ID, comment.id, comment.date.strftime("%Y-%m-%d %H:%M:%S"), comment.from_id.user_id, comment.message))
        
        return comments

    def test(self):
        messages = self._get_messages()

        for message in messages:
            entities = message.entities
            print(message.message, entities)

    def close(self):
        self.client.disconnect()


def main():
    with open('list_channels.txt', 'r') as f:
        channels = f.readlines()
    clear_channels = [chan.strip() for chan in channels]

    for chan in tqdm(clear_channels):


        try:
            tele_stat = Stat(chan)
            data_posts = tele_stat.get_posts_data()
            cur_df_posts = pd.DataFrame(data_posts)
            cur_df_posts.columns = ['CHAT_ID', 'message_id', 'message_date', 'message']
            #cur_df_posts.to_excel(r'data\\' + chan + '.xlsx')

            #cur_df_posts.to_csv(r'data\\' + chan + '.csv', sep='\t')
            with open(r'data\\' + chan + '.pickle', 'wb') as f:
                pickle.dump(cur_df_posts, f)
            print('\n', cur_df_posts.shape[0])
            tele_stat.close()
            del tele_stat
            time.sleep(1)
        except Exception as e:
            print('Error:\n', traceback.format_exc())
            print(f'\nNo user has {chan} as username')
            try:
                tele_stat.close()
                del tele_stat
                time.sleep(12)
            except:
                print('\n\nError on get channel info\n\n')


if __name__ == "__main__":
    main()