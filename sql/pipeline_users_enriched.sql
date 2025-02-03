BEGIN;

TRUNCATE TABLE users_enriched;

WITH users_deduplicated AS (
    SELECT DISTINCT ON (email)
        id, email, age, gender, country, city, traffic_source,
        FIRST_VALUE(traffic_source) 
            OVER (PARTITION BY email ORDER BY created_at ASC) 
            AS first_traffic_source,
        LAST_VALUE(traffic_source) 
            OVER (PARTITION BY email ORDER BY created_at ASC) 
            AS last_traffic_source,
        MIN(created_at) OVER (PARTITION BY email) AS created_at
    FROM users
    ORDER BY email, created_at DESC
), email_orders AS (
    SELECT u.email, oie.order_id, 
        MIN(oie.created_at) AS created_at,
        COUNT(*) AS n_order_items,
        SUM(oie.sale_price) AS order_value,
        ARRAY_AGG(oie.product_category) AS purchased_categories
    FROM order_items_enriched AS oie
    LEFT JOIN users AS u
    ON oie.user_id = u.id
    GROUP BY u.email, oie.order_id
    ORDER BY email, created_at
), email_orders_times AS (
    SELECT *,
        EXTRACT(DAY FROM 
            created_at - LAG(created_at)
            OVER (PARTITION BY email ORDER BY created_at ASC)
        ) AS days_to_order
    FROM email_orders
), email_purchases AS (
    SELECT email,
        COUNT(DISTINCT order_id) AS n_orders,
        AVG(n_order_items) AS avg_order_items, 
        SUM(order_value) / SUM(n_order_items) AS avg_item_value,
        AVG(order_value) AS avg_order_value,
        MIN(created_at) AS first_order_date,
        MAX(created_at) AS last_order_date,
        AVG(days_to_order) AS avg_days_to_order,
        COALESCE(STDDEV(days_to_order), 0) AS std_days_to_order
    FROM email_orders_times
    GROUP BY email
), email_first_order_cats AS (
    SELECT DISTINCT ON(email)
        email,
        FIRST_VALUE(purchased_categories)
            OVER (PARTITION BY email ORDER BY created_at ASC)
            AS first_order_categories
    FROM email_orders
), email_purchased_cats AS (
    SELECT
        email,
        ARRAY_AGG(unnested_purchased_cats) AS purchased_categories
    FROM email_orders
    LEFT JOIN LATERAL UNNEST(purchased_categories) AS unnested_purchased_cats ON true
    GROUP BY email
), users_with_purchases AS (
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
        u.first_traffic_source,
        u.last_traffic_source,
        u.created_at,
        p.first_order_date,
        p.last_order_date,
        EXTRACT(DAY FROM 
            p.first_order_date - u.created_at
        ) AS days_to_activation,
        EXTRACT(DAY FROM 
            p.last_order_date - p.first_order_date
        ) AS active_days,
        p.avg_days_to_order,
        p.std_days_to_order,
        COALESCE(p.n_orders, 0) AS n_orders,
        p.avg_order_items AS avg_order_items,
        p.avg_item_value AS avg_item_value,
        p.avg_order_value AS avg_order_value,
        foc.first_order_categories,
        pc.purchased_categories
    FROM users_deduplicated AS u
    LEFT JOIN email_purchases AS p
    ON u.email = p.email
    LEFT JOIN email_first_order_cats AS foc
    ON u.email = foc.email
    LEFT JOIN email_purchased_cats AS pc
    ON u.email = pc.email
    ORDER BY created_at
)

INSERT INTO users_enriched (
    id, age, gender, country, city, 
    first_traffic_source, last_traffic_source, 
    created_at, first_order_date, last_order_date, 
    days_to_activation, active_days, 
    avg_days_to_order, std_days_to_order, 
    n_orders, avg_order_items, avg_item_value, avg_order_value, 
    first_order_categories, purchased_categories
)
SELECT id, age, gender, country, city, 
    first_traffic_source, last_traffic_source, 
    created_at, first_order_date, last_order_date, 
    days_to_activation, active_days, 
    avg_days_to_order, std_days_to_order, 
    n_orders, avg_order_items, avg_item_value, avg_order_value, 
    first_order_categories, purchased_categories
FROM users_with_purchases;

COMMIT;





