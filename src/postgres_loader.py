import psycopg2
from config import POSTGRES_CONFIG
import logging
logger = logging.getLogger(__name__)

def get_pg_connection():
    return psycopg2.connect(**POSTGRES_CONFIG)


def load_csv_to_staging(csv_path, table_name):
    conn = get_pg_connection()
    try:
        with conn.cursor() as cur:
            logger.info(f"Loading {csv_path} into {table_name}")
            with open(csv_path, "r", encoding="utf-8") as f:
                cur.copy_expert(
                    """
                    COPY staging_fact_orders (
                        order_id,
                        customer_id,
                        order_status,
                        order_purchase_timestamp,
                        order_approved_at,
                        order_delivered_carrier_date,
                        order_delivered_customer_date,
                        order_estimated_delivery_date,
                        order_item_id,
                        product_id,
                        seller_id,
                        shipping_limit_date,
                        price,
                        freight_value,
                        total_price,
                        customer_unique_id,
                        customer_zip_code_prefix,
                        customer_city,
                        customer_state,
                        product_category_name,
                        product_name_lenght,
                        product_description_lenght,
                        product_photos_qty,
                        product_weight_g,
                        product_length_cm,
                        product_height_cm,
                        product_width_cm
                    )
                    FROM STDIN
                    WITH CSV HEADER
                    """,
                    f
                )
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("Failed loading CSV to staging")
        raise
    finally:
        conn.close()

def run_sql(sql):
    conn = get_pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
