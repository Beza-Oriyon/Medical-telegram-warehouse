# dashboard/app.py
"""
Medical Telegram Insights Dashboard
Interactive exploration of Telegram medical/cosmetics data.
"""

import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from dotenv import load_dotenv
import os
import logging

# ────────────────────────────────────────────────
# Logging Setup – safe & no side-effects
# ────────────────────────────────────────────────
def setup_logging():
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Clear old handlers (prevents duplicates on hot-reload)
    logger.handlers.clear()
    
    # File handler
    file_handler = logging.FileHandler(os.path.join(log_dir, "dashboard.log"))
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    logging.info("Logging configured successfully")

setup_logging()

# ────────────────────────────────────────────────
# Database Connection
# ────────────────────────────────────────────────
@st.cache_resource
def get_engine():
    load_dotenv()
    
    user = os.getenv("POSTGRES_USER", "postgres")
    pw = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "medical_warehouse")
    
    if not pw:
        st.error("POSTGRES_PASSWORD missing in .env")
        st.stop()
    
    url = f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}"
    
    try:
        engine = create_engine(url)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logging.info("Database connection successful")
        return engine
    except OperationalError as e:
        st.error("Database connection failed")
        st.error(str(e))
        logging.error(f"DB connection error: {e}")
        st.stop()

engine = get_engine()

# ────────────────────────────────────────────────
# Dashboard UI
# ────────────────────────────────────────────────
st.set_page_config(page_title="Medical Telegram Insights", layout="wide")

st.title("Medical Telegram Insights Dashboard")
st.markdown("""
Explore insights from Ethiopian Telegram channels (medical, pharma, cosmetics).  
Data from staging → dbt star schema → YOLO object enrichment.
""")

st.success("Connected to database!")

tab1, tab2, tab3 = st.tabs([
    "Top Mentioned Products/Drugs",
    "Visual Content by Channel (YOLO)",
    "Posting Trends"
])

with tab1:
    st.subheader("Top Mentioned Products/Drugs")
    limit = st.slider("Number of results", 5, 20, 10)

    @st.cache_data(ttl=600)
    def get_top_products(lim):
        query = """
            SELECT message_text, COUNT(*) as count
            FROM fct_messages
            WHERE message_text ILIKE '%%pill%%' OR message_text ILIKE '%%cream%%'
               OR message_text ILIKE '%%drug%%' OR message_text ILIKE '%%medicine%%'
            GROUP BY message_text
            ORDER BY count DESC
            LIMIT %s
        """
        return pd.read_sql(query, engine, params=(lim,))

    df = get_top_products(limit)
    if not df.empty:
        # Shorten long labels for chart readability
        df['short_text'] = df['message_text'].str[:30] + '...'
        st.bar_chart(df.set_index("short_text")["count"])
        st.dataframe(df[["message_text", "count"]])
    else:
        st.info("No matching product mentions found (check fct_messages).")

with tab2:
    st.subheader("Visual Content by Channel Category (YOLO)")
    @st.cache_data(ttl=600)
    def get_visual_stats():
        query = """
            SELECT dc.channel_category,
                   SUM(f.object_count) as total_objects,
                   AVG(f.object_count) as avg_objects,
                   COUNT(*) as messages_with_images
            FROM fct_messages f
            JOIN dim_channels dc ON f.channel_name = dc.channel_name
            WHERE f.object_count > 0
            GROUP BY dc.channel_category
            ORDER BY total_objects DESC
        """
        return pd.read_sql(query, engine)

    df_vis = get_visual_stats()
    if not df_vis.empty:
        st.bar_chart(df_vis.set_index("channel_category")["total_objects"])
        st.dataframe(df_vis)
    else:
        st.info("No YOLO detections yet (run enrich_images_yolo.py).")

with tab3:
    st.subheader("Posting Volume Trends")
    
    @st.cache_data(ttl=600)
    def get_trends():
        query = """
            SELECT year, month, COUNT(*) as count
            FROM fct_messages f
            JOIN dim_dates d ON f.date_key = d.date_key
            GROUP BY year, month
            ORDER BY year, month
        """
        return pd.read_sql(query, engine)

    df_trend = get_trends()
    
    if not df_trend.empty:
        # Create single x-axis column (Streamlit likes this)
        df_trend['year_month'] = df_trend['year'].astype(str) + '-' + df_trend['month'].astype(str).str.zfill(2)
        
        # Use single index → no KeyError
        st.line_chart(df_trend.set_index('year_month')['count'])
        
        # Show full table
        st.dataframe(df_trend[['year', 'month', 'count']])
    else:
        st.info("No trend data available yet.")