{{ config(materialized='table') }}

SELECT
    m.message_id,
    dc.channel_name,
    dc.channel_category,
    dd.date_key,
    m.message_timestamp,
    m.message_text,
    m.has_media,
    m.image_path,
    m.views,
    m.forwards,
    m.loaded_at,
    -- Placeholders for YOLO results (will update later)
    NULL::text[] AS detected_objects,
    NULL::integer AS object_count
FROM {{ ref('stg_telegram_messages') }} m
LEFT JOIN {{ ref('dim_channels') }} dc ON m.channel_name = dc.channel_name
LEFT JOIN {{ ref('dim_dates') }} dd ON DATE(m.message_timestamp) = dd.date_key