from pymongo import MongoClient, UpdateOne
import logging

logger = logging.getLogger(__name__)

class MongoRepository:

    def __init__(self, uri, db_name):
        self.uri = uri
        self.db_name = db_name

    def upsert_bulk(self, collection_name, df, id_fields, chunk_size=25000):

        logger.info(f"Mongo | Starting upsert into {collection_name}")

        client = MongoClient(self.uri)
        db = client[self.db_name]
        collection = db[collection_name]

        total_records = len(df)
        processed = 0

        operations = []

        try:
            for record in df.to_dict("records"):

                try:
                    record["_id"] = "_".join(str(record[field]) for field in id_fields)
                except KeyError as e:
                    logger.error(f"Missing key field: {e}")
                    raise ValueError(f"Missing key field: {e}")

                operations.append(
                    UpdateOne(
                        {"_id": record["_id"]},
                        {"$set": record},
                        upsert=True
                    )
                )

                if len(operations) >= chunk_size:
                    collection.bulk_write(operations, ordered=False)
                    processed += len(operations)
                    logger.info(f"Mongo | {collection_name}: {processed}/{total_records}")
                    operations = []

            if operations:
                collection.bulk_write(operations, ordered=False)
                processed += len(operations)

            logger.info(f"Mongo | Completed upsert into {collection_name} ({processed} records)")

        except Exception:
            logger.exception(f"Mongo | Failed upsert into {collection_name}")
            raise

        finally:
            client.close()
# class MongoRepository:

#     def __init__(self, uri: str, db_name: str, batch_size: int = 25000):
#         self.uri = uri
#         self.db_name = db_name
#         self.batch_size = batch_size

#     def _chunk(self, df):
#         for start in range(0, len(df), self.batch_size):
#             yield df.iloc[start:start + self.batch_size].to_dict("records")

#     def full_refresh(self, collection_name, df):
#         client = MongoClient(self.uri)

#         try:
#             db = client[self.db_name]

#             if collection_name in db.list_collection_names():
#                 db[collection_name].drop()

#             collection = db[collection_name]

#             total = len(df)
#             inserted = 0

#             for batch in self._chunk(df):
#                 collection.insert_many(batch, ordered=False)
#                 inserted += len(batch)
#                 logger.info(f"{collection_name}: {inserted}/{total}")

#         finally:
#             client.close()
        
#     def upsert_bulk(self, collection_name, df):

#         client = MongoClient(self.uri)
#         db = client[self.db_name]
#         collection = db[collection_name]

#         operations = []

#         for record in df.to_dict("records"):
#             record["_id"] = f"{record['order_id']}_{record['order_item_id']}"

#             operations.append(
#                 UpdateOne(
#                     {"_id": record["_id"]},
#                     {"$set": record},
#                     upsert=True
#                 )
#             )

#         if operations:
#             collection.bulk_write(operations)

#         client.close()

