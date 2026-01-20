{{ config(materialized='table') }}

SELECT DISTINCT
    channel_name,
    CASE
        WHEN channel_name = 'lobelia4cosmetics' THEN 'Cosmetics & Health Products'
        WHEN channel_name = 'tikvahethiopia' THEN 'General Promotions & News'
        WHEN channel_name = 'medicalethiopia' THEN 'Medical Education & Tips'
        WHEN channel_name = 'Thequorachannel' THEN 'Doctors & Health Announcements'
        ELSE 'Other'
    END AS channel_category
FROM {{ ref('stg_telegram_messages') }}