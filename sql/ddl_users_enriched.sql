CREATE TABLE users_enriched (
    id INT NOT NULL PRIMARY KEY,
    age INT,
    gender VARCHAR(255),
    country VARCHAR(255),
    city VARCHAR(255),
    traffic_source VARCHAR(255),
    purchases INT,
    revenue FLOAT,
    purchased_categories VARCHAR(255)[],
    created_at TIMESTAMP,
    first_purchase_date TIMESTAMP,
    last_purchase_date TIMESTAMP
)