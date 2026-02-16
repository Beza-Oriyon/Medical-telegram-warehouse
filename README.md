# Medical-telegram-warehouse
n end-to-end data pipeline for Telegram, leveraging dbt for transformation, Dagster for orchestration, and YOLOv8 for data enrichment.
# Week 8 Interim Submission - Task 1

- Scraped channels: @lobelia4cosmetics, @tikvahethiopia, etc.
- Raw data lake populated: data/raw/telegram_messages/ (partitioned by date/channel)
- Images downloaded to data/raw/images/
- Fields collected: message_id, channel_name, message_date, message_text, has_media, image_path, views, forwards
- Logs: logs/scraper.log
- Challenges: [e.g., flood waits handled]
---------------------------------------------
# Pharma Market Intelligence Pipeline – Week 12 Capstone Polish

**Base: Week 8 Telegram → dbt → YOLO → API pipeline**  
**Current focus: Reliability, testing, interactive dashboard for finance stakeholders**

**Business reframing (finance audience):**  
Tracking medical product mentions, prices, and visual patterns on Ethiopian Telegram channels to provide early market signals — useful for supply chain risk assessment, investment due diligence, or procurement forecasting in healthcare/pharma sector.

**Week 12 Progress Highlights (as of Feb 16, 2026):**
- Created dedicated branch for capstone improvements
- Added basic pytest unit tests for scraper logic
- Started interactive Streamlit dashboard (coming soon)
- Planning SHAP explainability on YOLO results
- CI workflow exists but needs fixes (old runs failing; local tests passing)

**Quick Local Run (for demo):**
```bash
# Activate env if using venv
# pip install -r requirements.txt   (if not done)

docker-compose up -d   # Start Postgres if needed

# Run dbt checks
cd medical_warehouse_dbt
dbt run --models staging+   # or full run if safe
dbt test

# Run tests
pytest ../tests/ -v