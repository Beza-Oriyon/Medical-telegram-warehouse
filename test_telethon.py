# test_telethon.py
import asyncio
from telethon import TelegramClient
from dotenv import load_dotenv
import os

load_dotenv()

api_id = int(os.getenv("TELEGRAM_API_ID"))
api_hash = os.getenv("TELEGRAM_API_HASH")
phone = os.getenv("TELEGRAM_PHONE")

client = TelegramClient('test_session', api_id, api_hash)

async def main():
    await client.start(phone=phone)
    me = await client.get_me()
    print("Connected successfully!")
    print(f"Your name: {me.first_name}")
    print(f"Username: @{me.username}" if me.username else "No username")

asyncio.run(main())