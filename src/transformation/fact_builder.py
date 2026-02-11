import pandas as pd


class FactBuilder:

    @staticmethod
    def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .str.replace(" ", "_")
        )
        return df

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self.normalize_columns(df)
        df = df.drop_duplicates()

        required = [c for c in ("order_id", "customer_id") if c in df.columns]
        if required:
            df = df.dropna(subset=required)

        numeric_cols = df.select_dtypes(include="number").columns
        df[numeric_cols] = df[numeric_cols].fillna(0)

        return df

    def build_fact(
        self,
        orders: pd.DataFrame,
        items: pd.DataFrame,
        customers: pd.DataFrame,
        products: pd.DataFrame
    ) -> pd.DataFrame:

        df = orders.merge(items, on="order_id", how="inner")
        df = df.merge(customers, on="customer_id", how="left")
        df = df.merge(products, on="product_id", how="left")

        if not {"price", "freight_value"}.issubset(df.columns):
            raise ValueError("Missing pricing columns")

        df["total_price"] = df["price"] + df["freight_value"]

        return df
