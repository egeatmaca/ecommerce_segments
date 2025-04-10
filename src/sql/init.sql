CREATE TABLE distribution_centers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL
);

CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    cost NUMERIC(10, 2) NOT NULL,
    category VARCHAR(255),
    name VARCHAR(255),
    brand VARCHAR(255),
    retail_price NUMERIC(10, 2),
    department VARCHAR(255),
    sku VARCHAR(255),
    distribution_center_id INT NOT NULL REFERENCES distribution_centers(id)
);

CREATE TABLE inventory_items (
    id SERIAL PRIMARY KEY,
    product_id INT NOT NULL REFERENCES products(id),
    created_at TIMESTAMP NOT NULL,
    sold_at TIMESTAMP,
    cost NUMERIC(10, 2) NOT NULL,
    product_category VARCHAR(255),
    product_name VARCHAR(255),
    product_brand VARCHAR(255),
    product_retail_price NUMERIC(10, 2),
    product_department VARCHAR(255),
    product_sku VARCHAR(255),
    product_distribution_center_id INT NOT NULL REFERENCES distribution_centers(id)
);

CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    email VARCHAR(255) NOT NULL,
    age INT,
    gender VARCHAR(10),
    state VARCHAR(50),
    street_address VARCHAR(255),
    postal_code VARCHAR(20),
    city VARCHAR(255),
    country VARCHAR(255),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    traffic_source VARCHAR(255),
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    user_id INT NOT NULL REFERENCES users(id),
    status VARCHAR(50),
    gender VARCHAR(10),
    created_at TIMESTAMP NOT NULL,
    returned_at TIMESTAMP,
    shipped_at TIMESTAMP,
    delivered_at TIMESTAMP,
    num_of_item INT NOT NULL
);

CREATE TABLE order_items (
    id SERIAL PRIMARY KEY,
    order_id INT NOT NULL REFERENCES orders(order_id),
    user_id INT NOT NULL REFERENCES users(id),
    product_id INT NOT NULL REFERENCES products(id),
    inventory_item_id INT REFERENCES inventory_items(id),
    status VARCHAR(50),
    created_at TIMESTAMP NOT NULL,
    shipped_at TIMESTAMP,
    delivered_at TIMESTAMP,
    returned_at TIMESTAMP,
    sale_price NUMERIC(10, 2)
);

CREATE TABLE events (
    id SERIAL PRIMARY KEY,
    user_id NUMERIC(10, 1),
    sequence_number INT NOT NULL,
    session_id VARCHAR(255),
    created_at TIMESTAMP NOT NULL,
    ip_address INET,
    city VARCHAR(255),
    state VARCHAR(255),
    postal_code VARCHAR(20),
    browser VARCHAR(255),
    traffic_source VARCHAR(255),
    uri TEXT,
    event_type VARCHAR(255)
);


COPY distribution_centers FROM '/docker-entrypoint-initdb.d/distribution_centers.csv' DELIMITER ',' CSV HEADER;
COPY products FROM '/docker-entrypoint-initdb.d/products.csv' DELIMITER ',' CSV HEADER;
COPY inventory_items FROM '/docker-entrypoint-initdb.d/inventory_items.csv' DELIMITER ',' CSV HEADER;
COPY users FROM '/docker-entrypoint-initdb.d/users.csv' DELIMITER ',' CSV HEADER;
COPY orders FROM '/docker-entrypoint-initdb.d/orders.csv' DELIMITER ',' CSV HEADER;
COPY order_items FROM '/docker-entrypoint-initdb.d/order_items.csv' DELIMITER ',' CSV HEADER;
COPY events FROM '/docker-entrypoint-initdb.d/events.csv' DELIMITER ',' CSV HEADER;