TRUNCATE TABLE fact_orders CASCADE;
TRUNCATE TABLE dim_customers CASCADE;
TRUNCATE TABLE dim_products CASCADE;

INSERT INTO dim_customers
SELECT DISTINCT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
FROM staging_fact_orders;

INSERT INTO dim_products
SELECT DISTINCT
    product_id,
    product_category_name,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm
FROM staging_fact_orders;

INSERT INTO fact_orders
SELECT
    order_id,
    customer_id,
    product_id,
    order_purchase_timestamp,
    total_price
FROM staging_fact_orders;
