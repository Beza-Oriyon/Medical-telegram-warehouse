"""
Manual Telethon connection verification script.
This file is NOT meant to be collected/run by pytest.

To run manually (only needed once or when session expires):
    python tests/test_telethon.py
"""

import asyncio
from telethon import TelegramClient
from dotenv import load_dotenv
import os

load_dotenv()

api_id = int(os.getenv("TELEGRAM_API_ID"))
api_hash = os.getenv("TELEGRAM_API_HASH")
phone = os.getenv("TELEGRAM_PHONE")

client = TelegramClient('test_session', api_id, api_hash)


async def verify_connection():
    """Connect to Telegram and print user info (manual run only)."""
    print("Starting manual Telethon connection test...")
    
    if not await client.is_user_authorized():
        print("Session not authorized. Starting login...")
        await client.start(phone=phone)
        print("Login complete. Session saved.")
    else:
        print("Reusing existing authorized session.")

    me = await client.get_me()
    print(f"Connected successfully as {me.first_name}")
    print(f"Username: @{me.username or 'no username'}")
    print(f"Phone: {me.phone}")


if __name__ == "__main__":
    asyncio.run(verify_connection())