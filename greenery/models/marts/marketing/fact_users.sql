{{
  config(
    materialized='table'
  )
}}

WITH users AS (
    SELECT 
        user_id,
        first_name,
        last_name,
        email,
        phone_number,
        created_at,
        updated_at,
        address_id
    FROM {{ ref('stg_users') }}
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
),

order_items AS (  
    SELECT 
        order_id,
        product_id,
        quantity
    FROM {{ ref('stg_order_items') }}
),

items_agg AS (
    SELECT 
        DISTINCT users.user_id,
        SUM(quantity) AS total_item_quantity,
        COUNT (DISTINCT product_id) AS unique_products_ordered
    FROM order_items
    JOIN orders
    ON orders.order_id = order_items.order_id
    JOIN users
    ON users.user_id = orders.user_id
    GROUP BY 1
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
    FROM {{ ref('stg_events') }}
),

event_agg_page_view AS (
    SELECT 
        DISTINCT user_id,
        COUNT (DISTINCT event_id) AS page_view_events
    FROM events
    WHERE event_type = 'page_view'
    GROUP BY 1
),

event_agg_checkout AS (
    SELECT 
        DISTINCT user_id,
        COUNT (DISTINCT event_id) AS checkout_events
    FROM events
    WHERE event_type = 'checkout'
    GROUP BY 1
),    
    
event_agg_package_shipped AS (
    SELECT 
        DISTINCT user_id,
        COUNT (DISTINCT event_id) AS package_shipped_events
    FROM events
    WHERE event_type = 'package_shipped'
    GROUP BY 1
),      

event_agg_add_to_cart AS (
    SELECT 
        DISTINCT user_id,
        COUNT (DISTINCT event_id) AS add_to_cart_events
    FROM events
    WHERE event_type = 'add_to_cart'
    GROUP BY 1
), 

event_agg_sessions AS (
    SELECT 
        DISTINCT user_id,
        COUNT (DISTINCT session_id) AS unique_session_count,
        COUNT (DISTINCT event_id) AS total_event_count
    FROM events
    GROUP BY 1
),  

addresses AS (
    SELECT
        address_id,
        address,
        zipcode,
        state,
        country
    FROM {{ ref('stg_addresses') }}
)

SELECT 
    u.user_id,
    u.first_name,
    u.last_name,
    u.email,
    u.phone_number,
    u.created_at,
    u.updated_at,
    u.address_id,
    a.address,
    a.zipcode,
    a.state,
    a.country,
    eas.unique_session_count, --may want to use NULLIFZERO here
    eas.total_event_count,    
    eapv.page_view_events,
    eac.checkout_events,
    eaps.package_shipped_events,
    eaatc.add_to_cart_events,
    ia.total_item_quantity,
    ia.unique_products_ordered, --may want to use NULLIFZERO here
    CASE 
        WHEN COUNT (DISTINCT o.order_id) = 0 THEN 'never_buyer'
        WHEN COUNT (DISTINCT o.order_id) = 1 THEN 'single_buyer'
        WHEN COUNT (DISTINCT o.order_id) >= 2 THEN 'repeat_buyer'
    END AS buyer_type,
    DATEDIFF('DAYS', MAX(o.created_at), SYSDATE()) AS days_since_last_order,
    COUNT (DISTINCT o.order_id) AS total_order_count,
    COALESCE(SUM(o.order_total),0) AS total_order_total,
    COALESCE(SUM(o.order_cost),0) AS total_order_cost,
    COALESCE(SUM(o.shipping_cost),0) AS total_shipping_cost,
    COALESCE((SUM(o.order_total) / COUNT (DISTINCT o.order_id)),0) AS avg_order_total,
    COALESCE((ia.total_item_quantity / COUNT (DISTINCT o.order_id)),0) AS avg_items_per_order,
    COALESCE((SUM(o.order_cost) / ia.total_item_quantity),0) AS avg_item_cost
    
FROM users u
LEFT JOIN orders o
    ON u.user_id = o.user_id
LEFT JOIN addresses a
    ON u.address_id = a.address_id
LEFT JOIN items_agg ia
    ON ia.user_id = u.user_id
LEFT JOIN event_agg_sessions eas
    ON eas.user_id = u.user_id
LEFT JOIN event_agg_page_view AS eapv
    ON eapv.user_id = u.user_id
LEFT JOIN event_agg_checkout AS eac
    ON eac.user_id = u.user_id
LEFT JOIN event_agg_package_shipped AS eaps
    ON eaps.user_id = u.user_id
LEFT JOIN event_agg_add_to_cart AS eaatc
    ON eaatc.user_id = u.user_id
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20