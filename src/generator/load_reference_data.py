"""
load_reference_data.py
-----------------------
Phase 1a: Load and validate the "reference" (batch, slow-changing) data from
the Olist dataset: customers, products, sellers.

This does NOT touch Kafka or MinIO yet — it's a local sanity check to confirm
the CSVs load cleanly and have the shape we expect before we build the
streaming replay script (orders/payments/events).

Run with: uv run python src/generator/load_reference_data.py
"""

import pandas as pd
from pathlib import Path

DATA_DIR = Path("data/raw_olist")


def load_customers() -> pd.DataFrame:
    df = pd.read_csv(DATA_DIR / "olist_customers_dataset.csv")
    return df


def load_products() -> pd.DataFrame:
    products = pd.read_csv(DATA_DIR / "olist_products_dataset.csv")
    translation = pd.read_csv(DATA_DIR / "product_category_name_translation.csv")
    products = products.merge(translation, on="product_category_name", how="left")
    return products


def load_sellers() -> pd.DataFrame:
    df = pd.read_csv(DATA_DIR / "olist_sellers_dataset.csv")
    return df


def summarize(name: str, df: pd.DataFrame) -> None:
    print(f"\n=== {name} ===")
    print(f"Rows: {len(df):,}")
    print(f"Columns: {list(df.columns)}")
    print(f"Nulls per column:\n{df.isnull().sum()}")
    print(f"Sample row:\n{df.iloc[0]}")


def main():
    customers = load_customers()
    products = load_products()
    sellers = load_sellers()

    summarize("Customers", customers)
    summarize("Products", products)
    summarize("Sellers", sellers)

    assert customers["customer_id"].is_unique, "Duplicate customer_id found!"
    assert products["product_id"].is_unique, "Duplicate product_id found!"
    assert sellers["seller_id"].is_unique, "Duplicate seller_id found!"

    print("\n✅ All reference data loaded and validated successfully.")


if __name__ == "__main__":
    main()
