{{
  config(
    materialized='table'
  )
}}

WITH promos AS (
    SELECT 
        promo_id,
        discount,
        status
    FROM {{ ref('stg_promos') }}
),

orders AS (  
    SELECT 
        order_id,
        user_id,
        promo_id,
        address_id,
        created_at,
        order_cost,
        shipping_cost,
        order_total,
        tracking_id,
        shipping_service,
        estimated_delivery_at,
        delivered_at,
        status
    FROM {{ ref('stg_orders') }}
)
    
SELECT 
    pr.promo_id,
    pr.discount,
    pr.status,
    COUNT (DISTINCT o.order_id) * pr.discount AS total_discount,
    COUNT (DISTINCT o.user_id) AS total_buyers_used,
    COUNT (DISTINCT o.order_id) AS total_orders
FROM promos pr
LEFT JOIN orders o
    ON pr.promo_id = o.promo_id
GROUP BY 1,2,3