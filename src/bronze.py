from pyspark.sql import SparkSession

from src.config import BRONZE_DIR, RAW_DIR
from src.schemas.olist_schemas import (
    customers_schema,
    geolocation_schema,
    order_items_schema,
    order_payments_schema,
    order_reviews_schema,
    orders_schema,
    products_schema,
    sellers_schema,
    category_translation_schema,
)


DATASETS = {
    "customers": (
        "olist_customers_dataset.csv",
        customers_schema,
    ),
    "geolocation": (
        "olist_geolocation_dataset.csv",
        geolocation_schema,
    ),
    "order_items": (
        "olist_order_items_dataset.csv",
        order_items_schema,
    ),
    "order_payments": (
        "olist_order_payments_dataset.csv",
        order_payments_schema,
    ),
    "order_reviews": (
        "olist_order_reviews_dataset.csv",
        order_reviews_schema,
    ),
    "orders": (
        "olist_orders_dataset.csv",
        orders_schema,
    ),
    "products": (
        "olist_products_dataset.csv",
        products_schema,
    ),
    "sellers": (
        "olist_sellers_dataset.csv",
        sellers_schema,
    ),
    "category_translation": (
        "product_category_name_translation.csv",
        category_translation_schema,
    ),
}


def run_bronze(spark: SparkSession):
    for dataset_name, (filename, schema) in DATASETS.items():

        input_path = RAW_DIR / filename
        output_path = BRONZE_DIR / dataset_name

        print(f"Processing Bronze dataset: {dataset_name}")

        df = (
            spark.read
            .option("header", True)
            .option("mode", "PERMISSIVE")
            .schema(schema)
            .csv(str(input_path))
        )

        row_count = df.count()

        (
            df.write
            .mode("overwrite")
            .parquet(str(output_path))
        )

        print(
            f"Bronze complete: {dataset_name} | "
            f"rows={row_count} | "
            f"output={output_path}"
        )

    print("Bronze layer completed successfully.")