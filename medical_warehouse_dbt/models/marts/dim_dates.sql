{{ config(materialized='table') }}

SELECT DISTINCT
    DATE(message_timestamp) AS date_key,
    EXTRACT(YEAR FROM message_timestamp) AS year,
    EXTRACT(MONTH FROM message_timestamp) AS month,
    EXTRACT(WEEK FROM message_timestamp) AS week,
    EXTRACT(DAY FROM message_timestamp) AS day,
    TO_CHAR(message_timestamp, 'Day') AS day_name,
    CASE
        WHEN EXTRACT(DOW FROM message_timestamp) IN (0,6) THEN 'Weekend'
        ELSE 'Weekday'
    END AS is_weekend
FROM {{ ref('stg_telegram_messages') }}
ORDER BY date_key