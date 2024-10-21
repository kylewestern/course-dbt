{{
  config(
    materialized='table'
  )
}}

WITH dates AS (
    SELECT 
        date
FROM {{ ref('dates') }}
),
        

agg_orders AS (  
    SELECT 
        created_at::date AS created_date,
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
        delivered_at::date AS delivery_date,
        COUNT (DISTINCT order_id) AS delivery_count,
    FROM {{ ref('stg_orders') }}
    WHERE delivered_at IS NOT NULL
    GROUP BY 1  
),

agg_users AS (
    SELECT 
        created_at::date AS created_date,
        COUNT (DISTINCT user_id) AS new_user_count
    FROM {{ ref('stg_users') }}        
    GROUP BY 1  
), 

agg_events AS (
    SELECT
        created_at::date AS created_date,
        COUNT (DISTINCT event_id) AS event_count,
        COUNT (DISTINCT user_id) AS event_user_count
    FROM {{ ref('stg_events') }}        
    GROUP BY 1  
)    
    
SELECT 
    d.date,
    COALESCE(ao.order_count,0) AS order_count,
    COALESCE(ao.buyer_count,0) AS buyer_count,
    COALESCE(ao.order_cost,0) AS order_cost,
    COALESCE(ao.order_total,0) AS order_total,
    COALESCE(ao.shipping_cost,0) AS shipping_cost,
    COALESCE(ad.delivery_count,0) AS delivery_count,
    COALESCE(au.new_user_count,0) AS new_user_count,
    COALESCE(ae.event_count,0) AS event_count,
    COALESCE(ae.event_user_count,0) AS event_user_count
    
FROM dates AS d
FULL JOIN agg_orders AS ao
    ON d.date = ao.created_date
FULL JOIN agg_deliveries AS ad
    ON d.date = ad.delivery_date
FULL JOIN agg_users AS au
    ON au.created_date = d.date
FULL JOIN agg_events ae
    ON ae.created_date = d.date
ORDER BY 1