BEGIN;

TRUNCATE TABLE users_enriched;

WITH user_purchases AS (
    SELECT user_id, 
        COUNT(*) AS purchases, 
        SUM(sale_price) AS revenue,
        MIN(created_at) AS first_purchase_date,
        MAX(created_at) AS last_purchase_date,
        ARRAY_AGG(product_category) AS purchased_categories
    FROM order_items_enriched
    GROUP BY user_id
), users_preped AS (
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
        COALESCE(up.purchases, 0) AS purchases,
        COALESCE(up.revenue, 0) AS revenue,
        up.purchased_categories,
        u.created_at,
        up.first_purchase_date,
        up.last_purchase_date
    FROM users AS u
    LEFT JOIN user_purchases AS up
    ON u.id = up.user_id
)

INSERT INTO users_enriched 
SELECT * FROM users_preped;

COMMIT;





