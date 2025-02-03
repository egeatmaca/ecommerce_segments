-- SELECT * FROM order_items_enriched LIMIT 100;
-- SELECT COUNT(*) FROM order_items_enriched;

SELECT * FROM users_enriched LIMIT 100;

-- SELECT MAX(cat_count) 
-- FROM (
--     SELECT COUNT(DISTINCT product_category) AS cat_count
--     FROM order_items_enriched 
--     GROUP BY order_id
-- ) LIMIT 100;

-- DROP TABLE order_items_enriched;
-- DROP TABLE users_enriched;