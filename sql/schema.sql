-- =========================
-- DROP ALL TABLES
-- =========================
DROP TABLE IF EXISTS fact_orders CASCADE;
DROP TABLE IF EXISTS dim_customers CASCADE;
DROP TABLE IF EXISTS dim_products CASCADE;
DROP TABLE IF EXISTS staging_fact_orders CASCADE;

-- =========================
-- STAGING TABLE
-- =========================
CREATE TABLE staging_fact_orders (

    -- Orders
    order_id VARCHAR,
    customer_id VARCHAR,
    order_status VARCHAR,
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,

    -- Order Items
    order_item_id INT,
    product_id VARCHAR,
    seller_id VARCHAR,
    shipping_limit_date TIMESTAMP,

    -- Pricing
    price NUMERIC,
    freight_value NUMERIC,
    total_price NUMERIC,

    -- Customers
    customer_unique_id VARCHAR,
    customer_zip_code_prefix INT,
    customer_city VARCHAR,
    customer_state VARCHAR,

    -- Products
    product_category_name VARCHAR,
    product_name_lenght INT,
   	product_description_lenght INT,
    product_photos_qty INT,
    product_weight_g INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT
);

-- =========================
-- DIM CUSTOMERS
-- =========================
CREATE TABLE dim_customers (
    customer_id VARCHAR PRIMARY KEY,
    customer_unique_id VARCHAR,
    customer_zip_code_prefix INT,
    customer_city VARCHAR,
    customer_state VARCHAR
);
-- =========================
-- DIM PRODUCTS
-- =========================
CREATE TABLE dim_products (
    product_id VARCHAR PRIMARY KEY,
    product_category_name VARCHAR,
    product_weight_g INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT
);
-- =========================
-- FACT ORDERS
-- =========================
CREATE TABLE fact_orders (
    order_id VARCHAR,
    customer_id VARCHAR,
    product_id VARCHAR,
    order_purchase_timestamp TIMESTAMP,
    total_price NUMERIC,

    PRIMARY KEY (order_id, product_id),

    FOREIGN KEY (customer_id)
        REFERENCES dim_customers(customer_id),

    FOREIGN KEY (product_id)
        REFERENCES dim_products(product_id)
);
-- =========================
-- INDEXES
-- =========================
CREATE INDEX idx_fact_orders_customer
    ON fact_orders(customer_id);

CREATE INDEX idx_fact_orders_product
    ON fact_orders(product_id);

CREATE INDEX idx_fact_orders_date
    ON fact_orders(order_purchase_timestamp);


SELECT column_name
FROM information_schema.columns
WHERE table_name = 'staging_fact_orders';
