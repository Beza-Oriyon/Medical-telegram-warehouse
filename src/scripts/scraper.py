import asyncio
import json
import os
from datetime import datetime
from pathlib import Path
import logging
from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument
from telethon.errors import FloodWaitError
import aiofiles

load_dotenv()

# Config
API_ID = int(os.getenv("TELEGRAM_API_ID"))
API_HASH = os.getenv("TELEGRAM_API_HASH")
PHONE = os.getenv("TELEGRAM_PHONE")

CHANNELS = [
    "lobelia4cosmetics",  # @lobelia4cosmetics
    # Add more: "chemed", "tikvahethiopia", etc. — without @
]

# Paths
RAW_DIR = Path("data/raw/telegram_messages")
IMAGES_DIR = Path("data/raw/images")
LOGS_DIR = Path("logs")
LOGS_DIR.mkdir(exist_ok=True)
logging.basicConfig(
    filename=LOGS_DIR / "scraper.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

client = TelegramClient("scraper_session", API_ID, API_HASH)

async def download_image(message, channel_name: str, message_date: datetime):
    if not message.media:
        return None

    img_dir = IMAGES_DIR / channel_name
    img_dir.mkdir(parents=True, exist_ok=True)

    file_ext = ".jpg"  # default
    if isinstance(message.media, MessageMediaPhoto):
        file_ext = ".jpg"
    elif isinstance(message.media, MessageMediaDocument) and message.media.document.mime_type.startswith("image/"):
        file_ext = f".{message.media.document.mime_type.split('/')[-1]}"
    else:
        return None  # not image

    filename = f"{message.id}_{message_date.strftime('%Y%m%d_%H%M%S')}{file_ext}"
    filepath = img_dir / filename

    if filepath.exists():
        logging.info(f"Image already exists: {filepath}")
        return str(filepath)

    try:
        await message.download_media(file=filepath)
        logging.info(f"Downloaded image: {filepath}")
        return str(filepath)
    except Exception as e:
        logging.error(f"Failed to download image {message.id}: {e}")
        return None

async def scrape_channel(channel_username: str, limit=50):  # limit=50 for testing; increase later
    entity = await client.get_entity(channel_username)
    channel_name = entity.username or entity.title.replace(" ", "_").lower()

    messages_data = []

    async for message in client.iter_messages(entity, limit=limit):
        if message.message is None:
            continue

        msg_date = message.date
        date_str = msg_date.strftime("%Y-%m-%d")
        partition_dir = RAW_DIR / date_str / channel_name
        partition_dir.mkdir(parents=True, exist_ok=True)

        image_path = await download_image(message, channel_name, msg_date)

        data = {
            "message_id": message.id,
            "channel_name": channel_name,
            "message_date": msg_date.isoformat(),
            "message_text": message.message,
            "has_media": message.media is not None,
            "image_path": image_path,
            "views": message.views or 0,
            "forwards": message.forwards or 0,
        }

        messages_data.append(data)

        # Save per message (or batch later)
        json_path = partition_dir / f"message_{message.id}.json"
        async with aiofiles.open(json_path, "w", encoding="utf-8") as f:
            await f.write(json.dumps(data, ensure_ascii=False, indent=2))

        logging.info(f"Saved message {message.id} from {channel_name}")

    return messages_data

async def main():
    await client.start(phone=PHONE)
    logging.info("Client started")

    for channel in CHANNELS:
        try:
            logging.info(f"Scraping {channel} ...")
            await scrape_channel(channel, limit=100)  # adjust limit
        except FloodWaitError as e:
            logging.warning(f"Flood wait: {e.seconds} seconds")
            await asyncio.sleep(e.seconds)
        except Exception as e:
            logging.error(f"Error scraping {channel}: {e}")

    await client.disconnect()
    logging.info("Scraping finished")

if __name__ == "__main__":
    asyncio.run(main())