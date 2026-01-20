from fastapi import FastAPI
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()

# Postgres connection
engine = create_engine(f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@localhost:5432/medical_warehouse")

app = FastAPI(title="Medical Telegram Analytical API")

@app.get("/")
def root():
    return {"message": "Welcome to the Medical Telegram Analytical API"}

@app.get("/top-products")
def top_products(limit: int = 10):
    """Top 10 most frequently mentioned medical products/drugs across all channels"""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT message_text, COUNT(*) as count
            FROM fct_messages
            WHERE message_text ILIKE '%pill%' OR message_text ILIKE '%cream%' OR message_text ILIKE '%drug%' 
               OR message_text ILIKE '%medicine%' OR message_text ILIKE '%cosmetic%'
            GROUP BY message_text
            ORDER BY count DESC
            LIMIT :limit
        """), {"limit": limit})
        return [dict(row) for row in result]

@app.get("/channel-visuals")
def channel_visuals():
    """Visual content stats by channel category (from YOLO detections)"""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT dc.channel_category, 
                   SUM(f.object_count) as total_objects,
                   AVG(f.object_count) as avg_objects_per_message,
                   COUNT(*) as message_count_with_images
            FROM fct_messages f
            JOIN dim_channels dc ON f.channel_name = dc.channel_name
            WHERE f.object_count > 0
            GROUP BY dc.channel_category
            ORDER BY total_objects DESC
        """))
        return [dict(row) for row in result]

@app.get("/trends")
def trends():
    """Daily and weekly posting volume trends"""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT d.year, d.month, d.week, d.day_name, d.is_weekend,
                   COUNT(*) as message_count
            FROM fct_messages f
            JOIN dim_dates d ON f.date_key = d.date_key
            GROUP BY d.year, d.month, d.week, d.day_name, d.is_weekend
            ORDER BY d.year, d.month, d.week
        """))
        return [dict(row) for row in result]

# Run with: uvicorn src.api.main:app --reload