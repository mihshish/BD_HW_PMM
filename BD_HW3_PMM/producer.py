import json
import asyncio
from telethon import TelegramClient, events
from telethon.tl.functions.channels import JoinChannelRequest
from kafka import KafkaProducer

api_id = 12345678           # api_id
api_hash = 'abcdefgh'  # api_hash

channels_to_listen = ['rbc_news', 'meduzalive', 'rian_ru', 'bbcrussian', 'rt_russian', 'tass_agency', 'oldlentach'] 

kafka_topic = 'telegram_data'
kafka_server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[kafka_server],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

client = TelegramClient('session_name', api_id, api_hash)

async def main():
    print("--- Запуск Producer ---")
    
    for channel in channels_to_listen:
        try:
            print(f"Подключаемся к каналу: {channel}")
            await client(JoinChannelRequest(channel))
        except Exception as e:
            print(f"Ошибка подключения к {channel}: {e}")

    print("Слушаем новые сообщения... (Не закрывайте это окно)")

    @client.on(events.NewMessage(chats=channels_to_listen))
    async def handler(event):
        message_text = event.message.message
        if message_text:
            data = {'text': message_text}
            producer.send(kafka_topic, value=data)
            print(f"[>>>] Отправлено в Kafka: {message_text[:40]}...")

    await client.run_until_disconnected()

if __name__ == '__main__':
    with client:
        client.loop.run_until_complete(main())