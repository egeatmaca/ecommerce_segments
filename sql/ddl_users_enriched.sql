CREATE TYPE order_item AS (
    order_id INTEGER,
    product_name TEXT,
    department TEXT,
    category TEXT,
    brand TEXT,
    sale_price FLOAT,
    created_at TIMESTAMP
);

CREATE TABLE users_enriched (
    id INT NOT NULL PRIMARY KEY,
    age INT,
    gender VARCHAR(255),
    country VARCHAR(255),
    city VARCHAR(255),
    traffic_source VARCHAR(255),
    created_at TIMESTAMP,
    first_order_timestamp TIMESTAMP,
    last_order_timestamp TIMESTAMP,
    days_to_activation INT,
    active_days INT,
    inactive_days INT,
    avg_days_to_order INT,
    std_days_to_order INT,
    n_orders INT,
    avg_order_items INT,
    max_order_items INT,
    avg_item_value FLOAT,
    max_item_value FLOAT,
    avg_order_value FLOAT,
    max_order_value FLOAT,
    order_items order_item[],
    lifetime_status VARCHAR(255),
    loyalty_segment VARCHAR(255),
    order_value_segment VARCHAR(255)
);
