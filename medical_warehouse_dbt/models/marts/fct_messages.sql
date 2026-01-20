{{ config(materialized='table') }}

SELECT
    m.message_id,
    dc.channel_name,
    dd.date_key,
    m.message_timestamp,
    m.message_text,
    m.has_media,
    m.image_path,
    m.views,
    m.forwards,
    -- Add placeholders for future YOLO enrichment
    NULL AS detected_objects,  -- e.g., array of ['pill', 'cream']
    NULL AS object_count
FROM {{ ref('stg_telegram_messages') }} m
LEFT JOIN {{ ref('dim_channels') }} dc ON m.channel_name = dc.channel_name
LEFT JOIN {{ ref('dim_dates') }} dd ON DATE(m.message_timestamp) = dd.date_key