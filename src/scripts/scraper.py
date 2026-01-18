import asyncio
import json
import os
from datetime import datetime
from pathlib import Path
import logging
from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument
from telethon.errors import FloodWaitError, ChannelPrivateError
import aiofiles

# Load environment variables
load_dotenv()

# Telegram API credentials from .env
API_ID = int(os.getenv("TELEGRAM_API_ID"))
API_HASH = os.getenv("TELEGRAM_API_HASH")
PHONE = os.getenv("TELEGRAM_PHONE")

# List of channels to scrape (without the @ prefix)
CHANNELS = [
    "lobelia4cosmetics",       # Cosmetics and health products – confirmed active
    "tikvahethiopia",          # Likely "Tikvah Pharma" – large Ethiopian channel with promotions
    # "chemed",                # CheMed – placeholder; search Telegram for exact @handle and uncomment
    "medicalethiopia",         # Medical Ethiopia – books, tips, videos (from et.tgstat.com/medicine)
    "Thequorachannel",         # Doctors Online Ethiopia – health news/announcements (from et.tgstat.com/medicine)
    # Add more from Telegram search or et.tgstat.com/medicine if needed
]

# Directories
RAW_DIR = Path("data/raw/telegram_messages")
IMAGES_DIR = Path("data/raw/images")
LOGS_DIR = Path("logs")
LOGS_DIR.mkdir(exist_ok=True)

# Logging setup
logging.basicConfig(
    filename=LOGS_DIR / "scraper.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Telethon client
client = TelegramClient("scraper_session", API_ID, API_HASH)

async def download_image(message, channel_name: str, message_date: datetime):
    """Download image if present and return path."""
    if not message.media:
        return None

    img_dir = IMAGES_DIR / channel_name
    img_dir.mkdir(parents=True, exist_ok=True)

    file_ext = ".jpg"
    if isinstance(message.media, MessageMediaPhoto):
        file_ext = ".jpg"
    elif isinstance(message.media, MessageMediaDocument) and message.media.document.mime_type.startswith("image/"):
        file_ext = f".{message.media.document.mime_type.split('/')[-1]}"
    else:
        return None  # Skip non-image media

    filename = f"{message.id}_{message_date.strftime('%Y%m%d_%H%M%S')}{file_ext}"
    filepath = img_dir / filename

    if filepath.exists():
        logging.info(f"Image already downloaded: {filepath}")
        return str(filepath)

    try:
        await message.download_media(file=str(filepath))
        logging.info(f"Downloaded image: {filepath}")
        return str(filepath)
    except Exception as e:
        logging.error(f"Image download failed for msg {message.id} in {channel_name}: {e}")
        return None

async def scrape_channel(channel_username: str, limit: int = 100):
    """Scrape messages from one channel."""
    try:
        entity = await client.get_entity(channel_username)
        channel_name = (entity.username or entity.title).replace(" ", "_").lower()

        logging.info(f"Starting scrape for @{channel_username} (display name: {channel_name})")

        messages_data = []

        async for message in client.iter_messages(entity, limit=limit):
            if message.message is None:
                continue  # Skip empty messages

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

            # Save each message as individual JSON (easy for partitioning & idempotency)
            json_path = partition_dir / f"message_{message.id}.json"
            async with aiofiles.open(json_path, "w", encoding="utf-8") as f:
                await f.write(json.dumps(data, ensure_ascii=False, indent=2))

            logging.info(f"Saved message {message.id} from @{channel_username}")

        logging.info(f"Finished scraping @{channel_username} - {len(messages_data)} messages collected")
        return messages_data

    except ChannelPrivateError:
        logging.error(f"Channel @{channel_username} is private or inaccessible")
    except FloodWaitError as e:
        logging.warning(f"Flood wait for @{channel_username}: sleeping {e.seconds} seconds")
        await asyncio.sleep(e.seconds)
    except Exception as e:
        logging.error(f"Error scraping @{channel_username}: {e}")
    return []

async def main():
    await client.start(phone=PHONE)
    logging.info("Telegram client started successfully")

    all_data = []
    for channel in CHANNELS:
        try:
            channel_data = await scrape_channel(channel, limit=100)  # Increase to 500/None for more data
            all_data.extend(channel_data)
            await asyncio.sleep(15)  # Pause between channels to reduce flood risk
        except Exception as e:
            logging.error(f"Failed processing channel {channel}: {e}")

    await client.disconnect()
    logging.info(f"Scraping completed. Total messages: {len(all_data)}")

if __name__ == "__main__":
    asyncio.run(main())