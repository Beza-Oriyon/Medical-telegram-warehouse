import json
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
from tqdm import tqdm
import logging

# Setup basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/loader.log"),
        logging.StreamHandler()
    ]
)

load_dotenv()

# Database connection from .env
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "medical_warehouse")

# Connection string
DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

# Staging table name
STAGING_TABLE = "staging_telegram_messages"

# Path to raw partitioned JSON data
RAW_DIR = Path("data/raw/telegram_messages")

def create_staging_table():
    """Create the staging table if it doesn't exist."""
    create_sql = """
    CREATE TABLE IF NOT EXISTS staging_telegram_messages (
        message_id BIGINT PRIMARY KEY,
        channel_name TEXT NOT NULL,
        message_date TIMESTAMP WITH TIME ZONE,
        message_text TEXT,
        has_media BOOLEAN,
        image_path TEXT,
        views INTEGER,
        forwards INTEGER,
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    try:
        with engine.connect() as conn:
            conn.execute(text(create_sql))
            conn.commit()  # Explicit commit required for DDL in PostgreSQL
        logging.info(f"Staging table '{STAGING_TABLE}' created or already exists.")
    except Exception as e:
        logging.error(f"Failed to create staging table: {e}")
        raise

def load_json_files():
    """Collect all JSON files recursively and load into a DataFrame."""
    json_files = list(RAW_DIR.rglob("*.json"))  # Find all message_*.json files
    if not json_files:
        logging.warning("No JSON files found in data/raw/telegram_messages/")
        return pd.DataFrame()

    all_data = []
    for file_path in tqdm(json_files, desc="Loading JSON files"):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                all_data.append(data)
        except json.JSONDecodeError as e:
            logging.error(f"Invalid JSON in {file_path}: {e}")
        except Exception as e:
            logging.error(f"Error reading {file_path}: {e}")

    if not all_data:
        logging.warning("No valid data loaded from JSON files.")
        return pd.DataFrame()

    df = pd.DataFrame(all_data)
    logging.info(f"Loaded {len(df)} messages from {len(json_files)} JSON files.")
    return df

def load_to_postgres(df: pd.DataFrame):
    """Load DataFrame to PostgreSQL staging table (append mode)."""
    if df.empty:
        logging.info("No data to load into database.")
        return

    try:
        # Append mode - safe for incremental loads
        df.to_sql(
            name=STAGING_TABLE,
            con=engine,
            if_exists="append",
            index=False,
            method="multi",  # faster batch insert
            chunksize=1000   # adjust if memory issues
        )
        logging.info(f"Successfully loaded {len(df)} rows into {STAGING_TABLE}.")
    except Exception as e:
        logging.error(f"Database load failed: {e}")
        raise

if __name__ == "__main__":
    logging.info("Starting data load to PostgreSQL staging...")
    create_staging_table()
    df = load_json_files()
    load_to_postgres(df)
    logging.info("Data load process completed.")