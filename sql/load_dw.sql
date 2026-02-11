

INSERT INTO dim_customers
SELECT DISTINCT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix::int,
    customer_city,
    customer_state
FROM staging_fact_orders
ON CONFLICT (customer_id) DO NOTHING;

INSERT INTO dim_products
SELECT DISTINCT
    product_id,
    product_category_name,
    product_weight_g::numeric,
    product_length_cm::numeric,
    product_height_cm::numeric,
    product_width_cm::numeric
FROM staging_fact_orders
ON CONFLICT (product_id) DO NOTHING;


INSERT INTO fact_orders (
    order_id,
    order_item_id,
    customer_id,
    product_id,
    order_purchase_timestamp,
    total_price
)
SELECT
    order_id,
    NULLIF(order_item_id, '')::int,
    customer_id,
    product_id,
    NULLIF(order_purchase_timestamp, '')::timestamp,
    NULLIF(total_price, '')::numeric
FROM staging_fact_orders
ON CONFLICT (order_id, order_item_id) DO NOTHING;