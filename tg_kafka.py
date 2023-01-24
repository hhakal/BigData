# -*- coding: utf-8 -*-

from telethon.sync import TelegramClient, events
from kafka import KafkaProducer
from telethon.tl.functions.channels import JoinChannelRequest, LeaveChannelRequest
import argparse

# Kafka topic name
TOPIC_NAME = "telegram-kafka"

# Kafka server
KAFKA_HOST = "localhost"
KAFKA_PORT = "9092"

api_id = 14341600
api_hash = '39b3c70e6a229cf3e423c679a17ac80a'

client = TelegramClient('sess', api_id, api_hash).start()



class KafkaCommunicator:
    def __init__(self, producer, topic):
        self.producer = producer
        self.topic = topic

    def send(self, message):
        mes_send = str(message)
        self.producer.send(self.topic, mes_send.encode("utf-8"))

    def close(self):
        self.producer.close()


class StreamListener():
    def __init__(self,
                 consumer_key,
                 consumer_secret,
                 access_token,
                 access_token_secret,
                 communicator):
        super().__init__(consumer_key, consumer_secret, access_token, access_token_secret)
        self.communicator = communicator

def create_communicator():
    """Create Kafka producer."""
    producer = KafkaProducer(bootstrap_servers=KAFKA_HOST + ":" + KAFKA_PORT)
    return KafkaCommunicator(producer, TOPIC_NAME)

async def listen():

    communicator = create_communicator()

    @client.on(events.NewMessage(chats=('tass_agency','interfaxonline','rian_ru','rt_russian','rentv_news','vestiru_24', 'ntvnews','news_1tv')))
    async def handler(event):
        mess = event.message.to_dict()['message']
        user = await client.get_entity(event.message.to_id)
        id_chat = user.id
        user_name = await client.get_entity(id_chat)
        channel = user_name.username
        data = {"chat_id": id_chat, "channel_name": channel, "message": mess}
        print(data)
        communicator.send(data)

parser = argparse.ArgumentParser()
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument("--j", action='store_true', help="Join channels")
group.add_argument("--lst", action='store_true', help="Listen channels")
group.add_argument("--lv", action='store_true', help="Leave from channels")
args = parser.parse_args()

async def join():
    await client(JoinChannelRequest('tass_agency'))
    await client(JoinChannelRequest('interfaxonline'))
    await client(JoinChannelRequest('rian_ru'))
    await client(JoinChannelRequest('rt_russian'))
    await client(JoinChannelRequest('rentv_news'))
    await client(JoinChannelRequest('vestiru24'))
    await client(JoinChannelRequest('ntvnews'))
    await client(JoinChannelRequest('news_1tv'))
    print("You are join to channels")

async def leave():
    await client(LeaveChannelRequest('tass_agency'))
    await client(LeaveChannelRequest('interfaxonline'))
    await client(LeaveChannelRequest('rian_ru'))
    await client(LeaveChannelRequest('rt_russian'))
    await client(LeaveChannelRequest('rentv_news'))
    await client(LeaveChannelRequest('vestiru24'))
    await client(LeaveChannelRequest('ntvnews'))
    await client(LeaveChannelRequest('news_1tv'))
    print("You are leave channels")

def main():
    if args.lst:
        with client:
            client.loop.run_until_complete(listen())
            client.run_until_disconnected()
    elif args.j:
        with client:
            client.loop.run_until_complete(join())
    elif args.lv:
        with client:
            client.loop.run_until_complete(leave())

if __name__ == '__main__':
    main()
