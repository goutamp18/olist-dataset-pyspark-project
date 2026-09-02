from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum


spark = (
    SparkSession.builder
    .appName("OlistAllDatasetsProfiling")
    .master("local[*]")
    .getOrCreate()
)


base_path = "data/raw"


datasets = {
    "customers": "olist_customers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "category_translation": "product_category_name_translation.csv"
}


for name, file_name in datasets.items():

    print("\n" + "=" * 70)
    print(f"DATASET: {name}")
    print("=" * 70)

    path = f"{base_path}/{file_name}"

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(path)
    )

    # Row count
    print(f"Rows: {df.count()}")

    # Column count
    print(f"Columns: {len(df.columns)}")

    # Column names
    print("Columns:")
    print(df.columns)

    # Schema
    print("\nSchema:")
    df.printSchema()

    # Sample
    print("\nSample:")
    df.show(3, truncate=False)

    # NULL counts
    print("\nNULL counts:")

    null_counts = df.select([
        sum(col(c).isNull().cast("int")).alias(c)
        for c in df.columns
    ])

    null_counts.show()


spark.stop()