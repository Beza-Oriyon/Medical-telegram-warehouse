{{ config(materialized='table') }}

SELECT
    message_id,
    channel_name,
    message_date::TIMESTAMP AS message_timestamp,
    TRIM(COALESCE(message_text, '')) AS message_text,
    has_media,
    image_path,
    COALESCE(views, 0) AS views,
    COALESCE(forwards, 0) AS forwards,
    loaded_at
FROM {{ source('raw_postgres', 'staging_telegram_messages') }}
WHERE message_id IS NOT NULL
  AND channel_name IS NOT NULL
  AND message_date IS NOT NULL