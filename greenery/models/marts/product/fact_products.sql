{{
  config(
    materialized='table'
  )
}}

WITH products AS (
SELECT 
    product_id,
    name,
    price,
    inventory
FROM {{ source('postgres', 'products') }}
),

order_items AS (
    SELECT        
        order_id,
        product_id,
        quantity    
FROM {{ source('postgres', 'order_items') }}
),

events AS (
    SELECT        
    event_id,
    session_id,
    user_id,
    page_url,
    created_at,
    event_type,
    order_id,
    product_id   
    FROM {{ source('postgres', 'events') }}
),

event_agg_page_view AS (
    SELECT 
        DISTINCT product_id,
        COUNT (DISTINCT event_id) AS page_view_events,
        COUNT (DISTINCT session_id) AS session_events
    FROM events
    WHERE 
        event_type = 'page_view'
        AND product_id IS NOT NULL
    GROUP BY 1
),


event_agg_add_to_cart AS (
    SELECT 
        DISTINCT product_id,
        COUNT (DISTINCT event_id) AS add_to_cart_events
    FROM events
    WHERE 
        event_type = 'add_to_cart'
        AND product_id IS NOT NULL
    GROUP BY 1
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
    FROM {{ source('postgres', 'orders') }}
),

event_agg_checkout AS (
    SELECT 
        DISTINCT oi.product_id,
        COUNT (DISTINCT e.session_id) AS checkout_sessions
    FROM events e
    JOIN orders o
        ON o.order_id = e.order_id
    JOIN order_items oi
        ON oi.order_id = o.order_id           
    WHERE 
        event_type = 'checkout'
        AND e.order_id IS NOT NULL
    GROUP BY 1
)

SELECT 
    p.product_id,
    p.name,
    p.price,
    p.inventory,
    eapv.page_view_events AS total_page_view_events,
    eapv.session_events AS total_session_events,
    eaatc.add_to_cart_events AS total_add_to_cart_events,
    (eaco.checkout_sessions::FLOAT / eapv.session_events) AS session_view_to_order_event_conversion_rate,
    (eaatc.add_to_cart_events::FLOAT / eapv.page_view_events) AS pv_to_cart_conversion_rate,
    (COUNT (DISTINCT oi.order_id)::FLOAT / eaatc.add_to_cart_events) AS cart_to_order_conversion_rate,
    (COUNT (DISTINCT oi.order_id)::FLOAT / eapv.page_view_events) AS pv_to_order_conversion_rate,
    COUNT (DISTINCT o.user_id) AS total_buyers,
    COUNT (DISTINCT oi.order_id) AS total_unique_orders,
    SUM(oi.quantity) AS total_quantity_ordered,
    SUM(oi.quantity) * p.price AS total_order_cost
    
FROM products p
LEFT JOIN order_items oi
    ON p.product_id = oi.product_id
LEFT JOIN orders o
    ON o.order_id = oi.order_id
LEFT JOIN event_agg_page_view AS eapv
    ON eapv.product_id = p.product_id
LEFT JOIN event_agg_add_to_cart AS eaatc
    ON eaatc.product_id = p.product_id
LEFT JOIN event_agg_checkout AS eaco
    ON eaco.product_id = p.product_id        
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8