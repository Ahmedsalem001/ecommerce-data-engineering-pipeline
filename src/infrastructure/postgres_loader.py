import logging
import pandas as pd
import psycopg2
from core.config import POSTGRES_CONFIG

logger = logging.getLogger(__name__)


class PostgresStagingLoader:

    def __init__(self, config: dict):
        self.config = config
        self.conn = None

    # ----------------------------
    # Connection Management
    # ----------------------------
    def connect(self):
        self.conn = psycopg2.connect(**self.config)

    def close(self):
        if self.conn:
            self.conn.close()

    # ----------------------------
    # Schema Helpers
    # ----------------------------
    @staticmethod
    def get_csv_columns(csv_path):
        df = pd.read_csv(csv_path, nrows=0)
        return list(df.columns)

    def get_table_columns(self, table_name):
        with self.conn.cursor() as cur: # type: ignore
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = %s
                ORDER BY ordinal_position
            """, (table_name,))
            return [row[0] for row in cur.fetchall()]

    @staticmethod
    def validate_schema(csv_cols, table_cols):
        if set(csv_cols) != set(table_cols):
            missing_in_db = set(csv_cols) - set(table_cols)
            missing_in_csv = set(table_cols) - set(csv_cols)

            raise ValueError(
                f"\nSchema mismatch detected!\n"
                f"Missing in DB: {missing_in_db}\n"
                f"Missing in CSV: {missing_in_csv}\n"
            )

    # ----------------------------
    # Load Method
    # ----------------------------
    def load_csv(self, csv_path, table_name):

        try:
            self.connect()

            # recreate staging table dynamically
            self.recreate_staging_from_csv(csv_path, table_name)

            with self.conn.cursor() as cur: # type: ignore
                with open(csv_path, "r", encoding="utf-8") as f:
                    cur.copy_expert(
                        f"""
                        COPY {table_name}
                        FROM STDIN
                        WITH CSV HEADER
                        """,
                        f
                    )

            self.conn.commit() # pyright: ignore[reportOptionalMemberAccess]

        except Exception:
            self.conn.rollback()
            raise

        finally:
            self.close()

    def recreate_staging_from_csv(self, csv_path, table_name):
        df = pd.read_csv(csv_path, nrows=0)
        columns = df.columns

        create_stmt = f"DROP TABLE IF EXISTS {table_name};\nCREATE TABLE {table_name} (\n"

        col_defs = []
        for col in columns:
            col_defs.append(f'    "{col}" TEXT')

        create_stmt += ",\n".join(col_defs)
        create_stmt += "\n);"

        with self.conn.cursor() as cur:
            cur.execute(create_stmt)

        self.conn.commit()
        
    def run_sql_file(self, sql_path):

        try:
            self.connect()

            with open(sql_path, "r", encoding="utf-8") as f:
                sql = f.read()
            with self.conn.cursor() as cur:
                cur.execute(sql)

            self.conn.commit()

        except Exception:
            self.conn.rollback()
            raise

        finally:
            self.close()

