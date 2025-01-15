BEGIN;

TRUNCATE TABLE order_items_enriched;

WITH inventory_items_brand_filled AS (
    SELECT 
        id,
        cost,
        product_category,
        product_department,
        CASE WHEN product_brand IS NULL THEN '<<NO_BRAND>>' ELSE product_brand END,
        product_name,
        product_distribution_center_id
    FROM inventory_items
), 
inventory_items_product_filled AS (
    SELECT 
        id,
        cost,
        product_category,
        product_department,
        product_brand,
        CASE 
            WHEN product_name IS NULL 
            THEN CONCAT(product_brand, ' - ', product_department, ' - ', product_category) 
            ELSE product_name 
        END,
        product_distribution_center_id
    FROM inventory_items_brand_filled
), 
inventory_items_preped AS (
    SELECT ii.*, dc.name AS distribution_center
    FROM inventory_items_product_filled AS ii
    JOIN distribution_centers AS dc
    ON ii.product_distribution_center_id = dc.id
),

-- Prepare users
users_preped AS (
    SELECT
        id,
        age AS user_age,
        gender AS user_gender,
        COALESCE(
            CASE WHEN country = 'España' THEN 'Spain' ELSE NULL END,
            CASE WHEN country = 'Deutschland' THEN 'Germany' ELSE NULL END,
            country
        ) AS user_country,
        city AS user_city,
        traffic_source AS user_traffic_source
    FROM users
),

-- Prepare enriched order items
order_items_preped AS (
    SELECT oi.id, oi.order_id, oi.user_id, oi.product_id, oi.inventory_item_id,
        oi.status, oi.created_at, oi.shipped_at, oi.delivered_at, oi.returned_at, oi.sale_price,
        ii.cost, ii.product_category, ii.product_department, ii.product_brand, ii.product_name, ii.distribution_center,
        u.user_age, u.user_gender, u.user_country, u.user_city, u.user_traffic_source
    FROM order_items AS oi
    LEFT JOIN inventory_items_preped AS ii
    ON oi.inventory_item_id = ii.id
    LEFT JOIN users_preped AS u
    ON oi.user_id = u.id
)

INSERT INTO order_items_enriched 
SELECT * FROM order_items_preped;

COMMIT;