{{
  config(
    materialized='table'
  )
}}

WITH agg_orders AS (  
    SELECT 
        'ltd' AS key,
        COUNT (DISTINCT order_id) AS order_count,
        COUNT (DISTINCT user_id) AS buyer_count,
        SUM(order_cost) AS order_cost,
        SUM(order_total) AS order_total,
        SUM(shipping_cost) AS shipping_cost
    FROM {{ ref('stg_orders') }}
    GROUP BY 1
),

agg_deliveries AS (
    SELECT 
        'ltd' AS key,
        COUNT (DISTINCT order_id) AS delivery_count,
    FROM {{ ref('stg_orders') }}
    WHERE delivered_at IS NOT NULL
    GROUP BY 1
),

agg_users AS (
    SELECT 
        'ltd' AS key,
        COUNT (DISTINCT user_id) AS registered_user_count
    FROM {{ ref('stg_users') }}    
    GROUP BY 1    
), 

agg_events AS (
    SELECT
        'ltd' AS key,
        COUNT (DISTINCT event_id) AS event_count,
        COUNT (DISTINCT session_id) AS session_count,
        COUNT (DISTINCT user_id) AS event_user_count
    FROM {{ ref('stg_events') }}        
    GROUP BY 1  
),

agg_checkout_events AS (
    SELECT
        'ltd' AS key,
        COUNT (DISTINCT event_id) AS checkout_event_count,
        COUNT (DISTINCT session_id) AS checkout_session_count,
        COUNT (DISTINCT user_id) AS checkout_user_count
    FROM {{ ref('stg_events') }}   
    WHERE event_type = 'checkout'
        AND order_id IS NOT NULL
    GROUP BY 1  
)
    
SELECT 
    COALESCE(ao.order_count,0) AS order_count,
    COALESCE(ao.buyer_count,0) AS buyer_count,
    COALESCE(ao.order_cost,0) AS order_cost,
    COALESCE(ao.order_total,0) AS order_total,
    COALESCE(ao.shipping_cost,0) AS shipping_cost,
    COALESCE(ad.delivery_count,0) AS delivery_count,
    COALESCE(au.registered_user_count,0) AS registered_user_count,
    COALESCE(ae.event_count,0) AS event_count,
    COALESCE(ae.event_user_count,0) AS event_user_count,
    (ace.checkout_session_count::FLOAT / ae.session_count) AS overall_conversion_rate
    
FROM agg_orders AS ao
FULL JOIN agg_deliveries AS ad
    ON ao.key = ao.key
FULL JOIN agg_users AS au
    ON au.key = ao.key
FULL JOIN agg_events ae
    ON ae.key = ao.key
FULL JOIN agg_checkout_events ace
    ON ace.key = ao.key
ORDER BY 1