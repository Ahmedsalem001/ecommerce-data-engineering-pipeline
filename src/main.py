import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

from core.config import (
    RAW_DATA_PATH,
    CLEAN_DATA_PATH,
    MONGO_URI,
    DB_NAME,
    POSTGRES_CONFIG
)

from ingestion.csv_loader import CSVLoader
from transformation.fact_builder import FactBuilder
from infrastructure.mongo_repository import MongoRepository
from infrastructure.postgres_loader import PostgresStagingLoader
from core.pipeline import ETLPipeline

paths = {
    "orders": RAW_DATA_PATH / "orders.csv",
    "items": RAW_DATA_PATH / "order_items.csv",
    "customers": RAW_DATA_PATH / "customers.csv",
    "products": RAW_DATA_PATH / "products.csv",
    "fact_csv": CLEAN_DATA_PATH / "fact_orders_clean.csv",
    "dw_sql": Path("sql/load_dw.sql"),
    "clean_dir": CLEAN_DATA_PATH
}
pipeline = ETLPipeline(
    csv_loader=CSVLoader(),
    fact_builder=FactBuilder(),
    mongo_repo=MongoRepository(MONGO_URI, DB_NAME),
    postgres_loader=PostgresStagingLoader(POSTGRES_CONFIG),
    paths=paths
)

pipeline.run()
