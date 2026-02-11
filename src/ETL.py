import logging
from pathlib import Path

import pandas as pd
from pymongo import MongoClient

from core.config import RAW_DATA_PATH, CLEAN_DATA_PATH, MONGO_URI, DB_NAME
from infrastructure.postgres_loader import PostgresStagingLoader
from core.config import POSTGRES_CONFIG
# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# Paths & Constants

ORDERS_FILE = RAW_DATA_PATH / "orders.csv"
ORDER_ITEMS_FILE = RAW_DATA_PATH / "order_items.csv"
CUSTOMERS_FILE = RAW_DATA_PATH / "customers.csv"
PRODUCTS_FILE = RAW_DATA_PATH / "products.csv"

FACT_CSV_PATH = CLEAN_DATA_PATH / "fact_orders_clean.csv"
STAGING_TABLE = "staging_fact_orders"

BATCH_SIZE = 1000


# IO Helpers

def load_csv(path: Path) -> pd.DataFrame:
    logger.info(f"Loading file: {path}")
    try:
        return pd.read_csv(path)
    except FileNotFoundError:
        logger.error(f"File not found: {path}")
        raise
    except Exception:
        logger.exception(f"Failed loading CSV: {path}")
        raise



# Data Preparation

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )
    return df


def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    df = normalize_columns(df)
    df = df.drop_duplicates()

    required_cols = [c for c in ("order_id", "customer_id") if c in df.columns]
    if required_cols:
        df = df.dropna(subset=required_cols)

    numeric_cols = df.select_dtypes(include="number").columns
    df[numeric_cols] = df[numeric_cols].fillna(0)

    return df


def build_fact_table(
    orders: pd.DataFrame,
    items: pd.DataFrame,
    customers: pd.DataFrame,
    products: pd.DataFrame
) -> pd.DataFrame:
    df = orders.merge(items, on="order_id", how="inner")
    df = df.merge(customers, on="customer_id", how="left")
    df = df.merge(products, on="product_id", how="left")
    return df


def transform_fact_table(df: pd.DataFrame) -> pd.DataFrame:
    required_cols = {"price", "freight_value"}
    if not required_cols.issubset(df.columns):
        raise ValueError("Missing pricing columns")

    df["total_price"] = df["price"] + df["freight_value"]
    return df



# Mongo Helpers

def get_mongo_client() -> MongoClient:
    return MongoClient(MONGO_URI)


def chunked(df: pd.DataFrame, size: int):
    for start in range(0, len(df), size):
        yield df.iloc[start:start + size].to_dict("records")


def full_refresh_to_mongo(collection_name: str, df: pd.DataFrame):
    client = get_mongo_client()
    try:
        db = client[DB_NAME]

        if collection_name in db.list_collection_names():
            logger.info(f"Dropping collection {collection_name}")
            db[collection_name].drop()

        collection = db[collection_name]
        total = len(df)

        logger.info(f"Loading {total} records into {collection_name}")

        inserted = 0
        for batch in chunked(df, BATCH_SIZE):
            collection.insert_many(batch, ordered=False)
            inserted += len(batch)
            logger.info(f"{collection_name}: {inserted}/{total}")

    except Exception:
        logger.exception(f"Failed loading {collection_name}")
        raise
    finally:
        client.close()

loader = PostgresStagingLoader(POSTGRES_CONFIG)

# Pipeline

def main():
    logger.info("Starting ETL pipeline")

    try:
        # Load
        orders = load_csv(ORDERS_FILE)
        items = load_csv(ORDER_ITEMS_FILE)
        customers = load_csv(CUSTOMERS_FILE)
        products = load_csv(PRODUCTS_FILE)
        # Raw layer
        full_refresh_to_mongo("orders_raw", orders)
        full_refresh_to_mongo("order_items_raw", items)
        full_refresh_to_mongo("customers_raw", customers)
        full_refresh_to_mongo("products_raw", products)

        # Clean
        orders = clean_df(orders)
        items = clean_df(items)
        customers = clean_df(customers)
        products = clean_df(products)
        # Fact
        fact_df = build_fact_table(orders, items, customers, products)
        fact_df = transform_fact_table(fact_df)

        # Save CSV
        CLEAN_DATA_PATH.mkdir(parents=True, exist_ok=True)
        fact_df.to_csv(FACT_CSV_PATH, index=False)

        # Clean layer
        full_refresh_to_mongo("fact_orders_clean", fact_df)

        # Data Warehouse

        loader.load_csv(FACT_CSV_PATH, "staging_fact_orders")
        
        logger.info("ETL pipeline completed successfully")

    except Exception:
        logger.exception("ETL pipeline failed")
        raise


if __name__ == "__main__":
    main()
