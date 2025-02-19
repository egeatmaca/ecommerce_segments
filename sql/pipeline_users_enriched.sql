BEGIN;

TRUNCATE TABLE users_enriched;

-- Deduplicate users based on email
WITH users_deduplicated AS (
    SELECT DISTINCT ON (email)
        id, email, age, gender, country, city, traffic_source,
        MIN(created_at) OVER (PARTITION BY email) AS created_at
    FROM users
    ORDER BY email, created_at DESC
), 
-- Join emails to order items
order_items_emails AS (
    SELECT oie.*, u.email
    FROM order_items_enriched AS oie
    LEFT JOIN users AS u
    ON oie.user_id = u.id
    WHERE oie.status <> 'Cancelled' AND oie.status <> 'Returned'
    ORDER BY u.email, oie.created_at
),
-- Agg. order items by (email, order_id)
email_orders AS (
    SELECT email, order_id,
        MIN(created_at) AS created_at,
        COUNT(*) AS n_order_items,
        SUM(sale_price) AS order_value
    FROM order_items_emails
    GROUP BY email, order_id
),
-- Calculate time-to-order
email_orders_times AS (
    SELECT *,
        EXTRACT(DAY FROM 
            created_at - LAG(created_at)
            OVER (PARTITION BY email ORDER BY created_at ASC)
        ) AS days_to_order
    FROM email_orders
),
--- Agg. orders by email
email_purchases AS (
    SELECT email,
        COUNT(DISTINCT order_id) AS n_orders,
        AVG(n_order_items) AS avg_order_items, 
        MAX(n_order_items) AS max_order_items, 
        AVG(order_value) AS avg_order_value, 
        MAX(order_value) AS max_order_value,
        MIN(created_at) AS first_order_timestamp,
        MAX(created_at) AS last_order_timestamp,
        AVG(days_to_order) AS avg_days_to_order,
        COALESCE(STDDEV(days_to_order), 0) AS std_days_to_order
    FROM email_orders_times
    GROUP BY email
),
-- Agg. items purchased by email
email_order_items AS (
    SELECT email,
        AVG(sale_price) AS avg_item_value,
        MAX(sale_price) AS max_item_value,
        ARRAY_AGG(
            ROW(
                order_id,
                product_name,
                product_department,
                product_category,
                product_brand,
                sale_price,
                created_at
            )::order_item
        ) AS order_items
    FROM order_items_emails
    GROUP BY email
),
-- Join deduplicated users and values aggregated by emails
users_with_purchases AS (
    SELECT
        u.id,
        u.age,
        u.gender,
        COALESCE(
            CASE WHEN u.country = 'España' THEN 'Spain' END,
            CASE WHEN country = 'Deutschland' THEN 'Germany' END,
            country
        ) AS country,
        u.city,
        u.traffic_source,
        u.created_at,
        p.first_order_timestamp,
        p.last_order_timestamp,
        EXTRACT(DAY FROM 
            p.first_order_timestamp - u.created_at
        ) AS days_to_activation,
        EXTRACT(DAY FROM 
            p.last_order_timestamp - p.first_order_timestamp
        ) AS active_days,
        EXTRACT(DAY FROM 
            CURRENT_TIMESTAMP - p.last_order_timestamp
        ) AS inactive_days,
        p.avg_days_to_order,
        p.std_days_to_order,
        COALESCE(p.n_orders, 0) AS n_orders,
        p.avg_order_items AS avg_order_items,
        p.max_order_items AS max_order_items,
        oi.avg_item_value AS avg_item_value,
        oi.max_item_value AS max_item_value,
        p.avg_order_value AS avg_order_value,
        p.max_order_value AS max_order_value,
        oi.order_items AS order_items
    FROM users_deduplicated AS u
    LEFT JOIN email_purchases AS p
    ON u.email = p.email
    LEFT JOIN email_order_items AS oi
    ON u.email = oi.email
    ORDER BY created_at
)

INSERT INTO users_enriched (
    id, age, gender, country, city, traffic_source,
    created_at, first_order_timestamp, last_order_timestamp, 
    days_to_activation, active_days, inactive_days,
    avg_days_to_order, std_days_to_order, 
    n_orders, avg_order_items, max_order_items, avg_item_value, 
    max_item_value, avg_order_value, max_order_value,
    order_items
)
SELECT id, age, gender, country, city, traffic_source,
    created_at, first_order_timestamp, last_order_timestamp, 
    days_to_activation, active_days, inactive_days,
    avg_days_to_order, std_days_to_order, 
    n_orders, avg_order_items, max_order_items, avg_item_value, 
    max_item_value, avg_order_value, max_order_value,
    order_items
FROM users_with_purchases;

COMMIT;





