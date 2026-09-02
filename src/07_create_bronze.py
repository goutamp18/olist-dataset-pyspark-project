from pyspark.sql import SparkSession

from schemas.olist_schemas import (
    customers_schema,
    geolocation_schema,
    order_items_schema,
    order_payments_schema,
    order_reviews_schema,
    orders_schema,
    products_schema,
    sellers_schema,
    category_translation_schema
)


spark = (
    SparkSession.builder
    .appName("OlistBronzeIngestion")
    .master("local[*]")
    .getOrCreate()
)


RAW_PATH = "data/raw"
BRONZE_PATH = "data/bronze"


datasets = {
    "customers": (
        "olist_customers_dataset.csv",
        customers_schema
    ),
    "geolocation": (
        "olist_geolocation_dataset.csv",
        geolocation_schema
    ),
    "order_items": (
        "olist_order_items_dataset.csv",
        order_items_schema
    ),
    "order_payments": (
        "olist_order_payments_dataset.csv",
        order_payments_schema
    ),
    "order_reviews": (
        "olist_order_reviews_dataset.csv",
        order_reviews_schema
    ),
    "orders": (
        "olist_orders_dataset.csv",
        orders_schema
    ),
    "products": (
        "olist_products_dataset.csv",
        products_schema
    ),
    "sellers": (
        "olist_sellers_dataset.csv",
        sellers_schema
    ),
    "category_translation": (
        "product_category_name_translation.csv",
        category_translation_schema
    )
}


for name, (filename, schema) in datasets.items():

    print("\n" + "=" * 70)
    print(f"PROCESSING: {name}")
    print("=" * 70)

    input_path = f"{RAW_PATH}/{filename}"
    output_path = f"{BRONZE_PATH}/{name}"

    df = (
        spark.read
        .option("header", True)
        .schema(schema)
        .csv(input_path)
    )

    print("Input rows:", df.count())

    (
        df.write
        .mode("overwrite")
        .parquet(output_path)
    )

    print(f"Bronze written to: {output_path}")


spark.stop()