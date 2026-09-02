from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    countDistinct,
    count,
    sum,
    avg,
    round,
    coalesce,
    lit
)

spark = (
    SparkSession.builder
    .appName("Gold Category")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

SILVER_PATH = "data/silver"
GOLD_PATH = "data/gold/category"


# ============================================================
# LOAD SILVER DATA
# ============================================================

products = spark.read.parquet(
    f"{SILVER_PATH}/products"
)

order_items = spark.read.parquet(
    f"{SILVER_PATH}/order_items"
)

category_translation = spark.read.parquet(
    f"{SILVER_PATH}/category_translation"
)


# ============================================================
# JOIN ORDER ITEMS WITH PRODUCTS
# ============================================================

items_with_products = (
    order_items
    .join(
        products.select(
            "product_id",
            "product_category_name"
        ),
        on="product_id",
        how="left"
    )
)


# ============================================================
# ADD ENGLISH CATEGORY NAME
# ============================================================

items_with_category = (
    items_with_products
    .join(
        category_translation,
        on="product_category_name",
        how="left"
    )
)


# ============================================================
# CATEGORY METRICS
# ============================================================

category_metrics = (
    items_with_category
    .groupBy(
        "product_category_name",
        "product_category_name_english"
    )
    .agg(
        countDistinct("product_id").alias(
            "unique_products"
        ),

        countDistinct("order_id").alias(
            "total_orders"
        ),

        count("*").alias(
            "total_items_sold"
        ),

        countDistinct("seller_id").alias(
            "unique_sellers"
        ),

        round(
            sum("price"),
            2
        ).alias(
            "total_product_revenue"
        ),

        round(
            sum("freight_value"),
            2
        ).alias(
            "total_freight_revenue"
        ),

        round(
            avg("price"),
            2
        ).alias(
            "average_item_price"
        )
    )
)


# ============================================================
# HANDLE NULL CATEGORY NAMES
# ============================================================

gold_category = (
    category_metrics
    .withColumn(
        "product_category_name_english",
        coalesce(
            col("product_category_name_english"),
            col("product_category_name")
        )
    )
)


# ============================================================
# SELECT FINAL COLUMNS
# ============================================================

gold_category = gold_category.select(
    "product_category_name",
    "product_category_name_english",
    "unique_products",
    "total_orders",
    "total_items_sold",
    "unique_sellers",
    "total_product_revenue",
    "total_freight_revenue",
    "average_item_price"
)


# ============================================================
# VALIDATION BEFORE WRITE
# ============================================================

print("\n========== GOLD CATEGORY VALIDATION ==========")

row_count = gold_category.count()

duplicate_categories = (
    gold_category
    .groupBy("product_category_name")
    .count()
    .filter(col("count") > 1)
    .count()
)

negative_revenue = (
    gold_category
    .filter(
        (col("total_product_revenue") < 0) |
        (col("total_freight_revenue") < 0)
    )
    .count()
)

invalid_metrics = (
    gold_category
    .filter(
        (col("unique_products") < 0) |
        (col("total_orders") < 0) |
        (col("total_items_sold") < 0) |
        (col("unique_sellers") < 0) |
        (col("average_item_price") < 0)
    )
    .count()
)

print(f"Category rows: {row_count}")
print(f"Duplicate categories: {duplicate_categories}")
print(f"Negative revenue records: {negative_revenue}")
print(f"Invalid metric records: {invalid_metrics}")


# ============================================================
# SAMPLE
# ============================================================

print("\n========== SAMPLE GOLD CATEGORIES ==========")

gold_category.show(
    20,
    truncate=False
)


# ============================================================
# WRITE GOLD DATA
# ============================================================

gold_category.write \
    .mode("overwrite") \
    .parquet(GOLD_PATH)

print(
    f"\nGold Category written to: {GOLD_PATH}"
)

print("\n========== GOLD CATEGORY COMPLETE ==========")

spark.stop()