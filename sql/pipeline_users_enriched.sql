BEGIN;

TRUNCATE TABLE users_enriched;

WITH user_purchases AS (
    SELECT user_id, 
        COUNT(DISTINCT order_id) AS n_orders,
        COUNT(*) AS n_order_items, 
        SUM(sale_price) AS revenue,
        MIN(created_at) AS first_purchase_date,
        MAX(created_at) AS last_purchase_date,
        ARRAY_AGG(product_category) AS purchased_categories
    FROM order_items_enriched
    GROUP BY user_id
), users_with_purchases AS (
    SELECT
        u.id,
        u.email,
        u.age,
        u.gender,
        COALESCE(
            CASE WHEN u.country = 'España' THEN 'Spain' END,
            CASE WHEN country = 'Deutschland' THEN 'Germany' END,
            country
        ) AS country,
        u.city,
        u.traffic_source,
        COALESCE(up.n_orders, 0) AS n_orders,
        COALESCE(up.n_order_items, 0) AS n_order_items,
        COALESCE(up.revenue, 0) AS revenue,
        up.purchased_categories,
        u.created_at,
        up.first_purchase_date,
        up.last_purchase_date
    FROM users AS u
    LEFT JOIN user_purchases AS up
    ON u.id = up.user_id
    ORDER BY created_at
), users_merged AS (
    SELECT DISTINCT ON (email)
        id, gender, country, city, traffic_source,
        FIRST_VALUE(age) OVER (PARTITION BY email ORDER BY created_at DESC) AS age,
        SUM(n_orders) OVER (PARTITION BY email) AS n_orders,
        SUM(n_order_items) OVER (PARTITION BY email) AS n_order_items,
        SUM(revenue) OVER (PARTITION BY email) AS revenue,
        MIN(created_at) OVER (PARTITION BY email) AS created_at,
        MIN(first_purchase_date) OVER (PARTITION BY email) AS first_purchase_date,
        MAX(last_purchase_date) OVER (PARTITION BY email) AS last_purchase_date
    FROM users_with_purchases
    ORDER BY email, created_at DESC
), cat_purchases_merged AS (
    SELECT DISTINCT ON (email)
        id,
        ARRAY_AGG(unnested_purchased_cats) OVER (PARTITION BY email) AS purchased_categories
    FROM users_with_purchases
    LEFT JOIN LATERAL UNNEST(purchased_categories) AS unnested_purchased_cats ON true
    ORDER BY email, created_at DESC
), users_preped AS (
    SELECT um.id, um.age, um.gender, um.country, um.city, um.traffic_source,
           um.n_orders, um.n_order_items, um.revenue, cpm.purchased_categories, 
           um.created_at, um.first_purchase_date, um.last_purchase_date
    FROM users_merged AS um
    LEFT JOIN cat_purchases_merged AS cpm
    ON um.id = cpm.id
)

INSERT INTO users_enriched 
SELECT * FROM users_preped;

COMMIT;





