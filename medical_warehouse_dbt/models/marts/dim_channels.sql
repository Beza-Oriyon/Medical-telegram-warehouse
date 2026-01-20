{{ config(materialized='table') }}

SELECT DISTINCT
    channel_name,
    CASE
        WHEN channel_name ILIKE '%lobelia%' THEN 'Cosmetics & Health Products'
        WHEN channel_name ILIKE '%tikvah%' THEN 'General Promotions & Pharma'
        WHEN channel_name ILIKE '%medical%' THEN 'Medical Education & Tips'
        WHEN channel_name ILIKE '%quora%' OR channel_name ILIKE '%doctor%' THEN 'Doctors & Health Announcements'
        ELSE 'Other / Unknown'
    END AS channel_category,
    COUNT(*) OVER (PARTITION BY channel_name) AS total_messages
FROM {{ ref('stg_telegram_messages') }}
ORDER BY total_messages DESC