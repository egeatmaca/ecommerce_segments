-- SELECT * FROM order_items_enriched LIMIT 100;
-- SELECT COUNT(*) FROM order_items_enriched;

-- SELECT * FROM users_enriched LIMIT 10000;

-- SELECT * 
-- FROM users_enriched 
-- WHERE cat_most_purchased <> cat_most_spent
-- LIMIT 10000;

-- SELECT COUNT(*)
-- FROM users_enriched 
-- WHERE category_most_purchased <> category_most_spent;

-- SELECT MAX(cat_count) 
-- FROM (
--     SELECT COUNT(DISTINCT product_category) AS cat_count
--     FROM order_items_enriched 
--     GROUP BY order_id
-- ) LIMIT 100;

-- DROP TABLE order_items_enriched;

-- DROP TABLE users_enriched;
-- DROP TYPE order_item;