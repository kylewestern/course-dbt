{{
  config(
    materialized='table'
  )
}}


WITH events AS (
    SELECT
        event_id,
        session_id,
        user_id,
        page_url,
        created_at,
        event_type,
        order_id,
        product_id
    FROM {{ ref('stg_events') }}
),

order_items AS (
    SELECT 
        order_id,
        product_id,
        quantity
    FROM {{ ref('stg_order_items') }}
),

session_timing_agg AS (
    SELECT *
    FROM {{ ref('int_session_timing') }}
)

{% set event_types = ['page_view', 'add_to_cart', 'checkout', 'package_shipped'] %}

SELECT 
    e.session_id,
    e.user_id,
    COALESCE(e.product_id, oi.product_id) AS product_id,
    s.session_started_at,
    s.session_ended_at,
    {% for event_type in event_types %}
    {{ sum_of('e.event_type', event_type ) }} AS {{ event_type }}s,
    {% endfor %}
    DATEDIFF('MINUTE', s.session_started_at, s.session_ended_at) AS session_length_minutes
FROM events e
LEFT JOIN order_items oi
    ON oi.order_id = e.order_id
LEFT JOIN session_timing_agg s
    ON s.session_id = e.session_id
GROUP BY 1, 2, 3, 4, 5