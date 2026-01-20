# Medical-telegram-warehouse
n end-to-end data pipeline for Telegram, leveraging dbt for transformation, Dagster for orchestration, and YOLOv8 for data enrichment.
# Week 8 Interim Submission - Task 1

- Scraped channels: @lobelia4cosmetics, @tikvahethiopia, etc.
- Raw data lake populated: data/raw/telegram_messages/ (partitioned by date/channel)
- Images downloaded to data/raw/images/
- Fields collected: message_id, channel_name, message_date, message_text, has_media, image_path, views, forwards
- Logs: logs/scraper.log
- Challenges: [e.g., flood waits handled]