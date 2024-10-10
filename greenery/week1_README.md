# Week 1 Project Questions

## 1. How many users do we have?

### Answer
We have **130** users.

### Query
```sql
SELECT COUNT (DISTINCT user_id) AS user_count
FROM DEV_DB.DBT_KWESTERNBETTERCOLLECTIVECOM.STG_USERS
```

## 2. On average, how many orders do we receive per hour?

### Answer
We receive an average of **7.5** orders per hour. (Rounded from 7.520833) 

### Query
```sql
WITH hourly_orders AS (

    SELECT 
        DATE_TRUNC('hour', created_at) AS hour_of_order, 
        COUNT (DISTINCT order_id) AS order_count 
    FROM DEV_DB.DBT_KWESTERNBETTERCOLLECTIVECOM.STG_ORDERS
    GROUP BY 1)

SELECT AVG(hourly_orders.order_count) AS avg_orders_per_hour
FROM hourly_orders
```

## 3. On average, how long does an order take from being placed to being delivered?

### Answer
On average, it takes **3 days, 21 hours, 24 minutes, and 11 seconds** for an order to be delivered after being placed.

> Note: Rounding up the seconds (11.8 -> 12), the display value would instead be "3 days, 21 hours, 24 minutes, and 12 seconds"

*Altenrative expressions:*
```
AVG_DELIVERY_TIME_SECONDS: 336252 (rounded from 336251.803279)

AVG_DELIVERY_TIME_MINUTES: 5604 (rounded from 5604.196721)

AVG_DELIVERY_TIME_HOURS: 93.4 (rounded from 93.403279)

AVG_DELIVERY_TIME_DAYS: 3.9 (rounded from 3.891803)
```

### Query
```sql
WITH orders_delivered AS (
    
    SELECT 
        order_id, 
        created_at, 
        delivered_at, 
        TIMESTAMPDIFF(second, created_at, delivered_at) AS delivery_time_seconds,
        TIMESTAMPDIFF(minute, created_at, delivered_at) AS delivery_time_minutes,
        TIMESTAMPDIFF(hour, created_at, delivered_at) AS delivery_time_hours,
        TIMESTAMPDIFF(day, created_at, delivered_at) AS delivery_time_days,
    FROM DEV_DB.DBT_KWESTERNBETTERCOLLECTIVECOM.STG_ORDERS
    WHERE status = 'delivered') --limits to only orders that have been delivered as shipped/processed orders will not yet have a delivered_at value

SELECT 
    AVG(delivery_time_seconds) AS avg_delivery_time_seconds,
    AVG(delivery_time_minutes) AS avg_delivery_time_minutes,
    AVG(delivery_time_hours) AS avg_delivery_time_hours,
    AVG(delivery_time_days) AS avg_delivery_time_days,
    FLOOR(avg_delivery_time_seconds/60/60/24)||' '||'days'||','||' '||FLOOR(avg_delivery_time_seconds/60/60%24)||' '||'hours'||','||' '||FLOOR(avg_delivery_time_seconds/60%60)||' '||'minutes'||', '||FLOOR(avg_delivery_time_seconds%60)||' '||'seconds' AS avg_delivery_time_display
FROM orders_delivered
```

## 4. How many users have only made one purchase? Two purchases? Three+ purchases?

### Answer
- **25** users have made exactly ONE purchase
- **28** users have made exactly TWO purchases
- **71** users have made THREE OR MORE purchases

### Query
```sql
WITH buyers AS (

    SELECT 
        DISTINCT user_id, 
        COUNT (DISTINCT order_id) AS order_count
    FROM DEV_DB.DBT_KWESTERNBETTERCOLLECTIVECOM.STG_ORDERS
    GROUP BY 1)

SELECT 
    CASE 
        WHEN buyers.order_count = 1 THEN 'exactly_one_order'
        WHEN buyers.order_count = 2 THEN 'exactly_two_orders'
        WHEN buyers.order_count >= 3 THEN 'three_or_more_orders'
    END AS orders_per_buyer_bucket,
    COUNT (buyers.user_id) AS number_of_buyers
FROM buyers
GROUP BY 1
ORDER BY 1
```    

## 5. On average, how many unique sessions do we have per hour?

### Answer
We have an average of **16.3** unique sessions per hour. (Rounded from 16.327586)

> **Note:** This represents the number of unique sessions that had at least one event in an hour rather than the number of sessions that were intitiated in the hour.



### Query

```sql
WITH hourly_sessions AS (
    
    SELECT 
        DATE_TRUNC('hour', created_at) AS hour_of_session, 
        COUNT (DISTINCT session_id) AS unique_session_count 
    FROM DEV_DB.DBT_KWESTERNBETTERCOLLECTIVECOM.STG_EVENTS
    GROUP BY 1)

SELECT AVG(hourly_sessions.unique_session_count) AS avg_unique_sessions_per_hour
FROM hourly_sessions
```
