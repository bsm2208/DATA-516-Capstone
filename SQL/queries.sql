-- query 1, conversion funnel
WITH event_counts AS (
    SELECT 
        product_id,
        SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) AS view_count,
        SUM (CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS cart_count,
        SUM (CASE WHEN event_type = 'purchase' THEN 1  ELSE 0 END) AS purchase_count
    FROM events
    WHERE event_type IN ('page_view','add_to_cart','purchase')
    GROUP BY product_id
)
SELECT 
    product_id, 
    view_count,
    cart_count,
    purchase_count,
    ROUND(cart_count* 100.0/view_count, 2) AS view_to_cart_rate,
    ROUND(purchase_count* 100.0/NULLIF(cart_count, 0), 2) AS cart_to_purchase_rate
FROM event_counts;


-- query 2, hourly revenue
SELECT year, month, day, hour, SUM(price * quantity) AS total_revenue
FROM events
WHERE event_type = 'purchase'
GROUP BY year, month, day, hour;

-- query 3, top 10 products
SELECT product_id, COUNT(*) AS number_of_views
FROM events
WHERE event_type = 'page_view'
GROUP BY product_id
ORDER BY number_of_views DESC
LIMIT 10;

-- query 4, Category performance
SELECT year, month, day, category, COUNT(*) AS event_count
FROM events
GROUP BY year, month, day, category;

-- query 5, User activity
SELECT year, month, day, COUNT(DISTINCT user_id) AS user_count , COUNT(DISTINCT session_id) AS session_count
FROM events
GROUP BY year, month, day;


