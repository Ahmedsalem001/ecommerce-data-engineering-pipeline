import logging
from pathlib import Path


logger = logging.getLogger(__name__)


class ETLPipeline:

    def __init__(
        self,
        csv_loader,
        fact_builder,
        mongo_repo,
        postgres_loader,
        paths
    ):
        self.csv_loader = csv_loader
        self.fact_builder = fact_builder
        self.mongo_repo = mongo_repo
        self.postgres_loader = postgres_loader
        self.paths = paths

    def run(self):
        logger.info("Starting ETL pipeline")

        # Load
        orders = self.csv_loader.load(self.paths["orders"])
        items = self.csv_loader.load(self.paths["items"])
        customers = self.csv_loader.load(self.paths["customers"])
        products = self.csv_loader.load(self.paths["products"])

        # Raw to Mongo
        # self.mongo_repo.full_refresh("orders_raw", orders)
        # self.mongo_repo.full_refresh("order_items_raw", items)
        # self.mongo_repo.full_refresh("customers_raw", customers)
        # self.mongo_repo.full_refresh("products_raw", products)
        self.mongo_repo.upsert_bulk("orders_raw", orders, id_fields=["order_id"])
        self.mongo_repo.upsert_bulk("order_items_raw", items, id_fields=["order_id", "order_item_id"])
        self.mongo_repo.upsert_bulk("customers_raw", customers, id_fields=["customer_id"])
        self.mongo_repo.upsert_bulk("products_raw", products, id_fields=["product_id"])
        # Clean
        orders = self.fact_builder.clean(orders)
        items = self.fact_builder.clean(items)
        customers = self.fact_builder.clean(customers)
        products = self.fact_builder.clean(products)

        # Build Fact
        fact = self.fact_builder.build_fact(
            orders, items, customers, products
        )

        # Save CSV
        self.paths["clean_dir"].mkdir(parents=True, exist_ok=True)
        fact.to_csv(self.paths["fact_csv"], index=False)

        # Clean to Mongo
        # self.mongo_repo.full_refresh("fact_orders_clean", fact)
        self.mongo_repo.upsert_bulk(
        collection_name="fact_orders_clean",
        df=fact,
        id_fields=["order_id", "order_item_id"]
        )

        # Load to Postgres
        self.postgres_loader.load_csv(
            self.paths["fact_csv"],
            "staging_fact_orders"
        )
        self.postgres_loader.run_sql_file(
            self.paths["dw_sql"]
            )   

        logger.info("ETL pipeline completed successfully")
