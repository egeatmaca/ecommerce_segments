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
        COALESCE(up.purchases, 0) AS purchases,
        COALESCE(up.revenue, 0) AS revenue,
        up.purchased_categories,
        u.created_at,
        up.first_purchase_date,
        up.last_purchase_date
    FROM users AS u
    LEFT JOIN user_purchases AS up
    ON u.id = up.user_id
    ORDER BY created_at
), users_duplicates_merged AS (
    SELECT id, age, gender, country, city, traffic_source, 
           purchases, revenue, purchased_categories,
           created_at, first_purchase_date, last_purchase_date 
    FROM (
        SELECT
            id, email, age, gender, country, city, traffic_source,
            SUM(purchases) OVER (PARTITION BY email) AS purchases,
            SUM(revenue) OVER (PARTITION BY email) AS revenue,
            ARRAY_AGG(unnested_purchased_cats) OVER (PARTITION BY email) AS purchased_categories,
            MIN(created_at) OVER (PARTITION BY email) AS created_at,
            MIN(first_purchase_date) OVER (PARTITION BY email) AS first_purchase_date,
            MAX(last_purchase_date) OVER (PARTITION BY email) AS last_purchase_date,
            ROW_NUMBER() OVER (PARTITION BY email ORDER BY created_at DESC) AS backward_duplicate_number
        FROM users_with_purchases
        LEFT JOIN LATERAL UNNEST(purchased_categories) as unnested_purchased_cats ON true
    )
    WHERE backward_duplicate_number=1
)

INSERT INTO users_enriched 
SELECT * FROM users_duplicates_merged;

COMMIT;





