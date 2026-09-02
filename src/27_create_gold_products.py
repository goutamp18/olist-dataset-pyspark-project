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
    .appName("Gold Products")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# Paths
# ============================================================

SILVER_PATH = "data/silver"
GOLD_PATH = "data/gold/products"


# ============================================================
# Read Silver datasets
# ============================================================

products = spark.read.parquet(f"{SILVER_PATH}/products")
order_items = spark.read.parquet(f"{SILVER_PATH}/order_items")


# ============================================================
# Aggregate order item metrics at product level
# ============================================================

product_metrics = (
    order_items
    .groupBy("product_id")
    .agg(
        countDistinct("order_id").alias("total_orders"),
        count("*").alias("total_items_sold"),
        countDistinct("seller_id").alias("unique_sellers"),
        round(sum("price"), 2).alias("total_product_revenue"),
        round(sum("freight_value"), 2).alias("total_freight_revenue"),
        round(avg("price"), 2).alias("average_item_price")
    )
)


# ============================================================
# Join product information with metrics
# ============================================================

gold_products = (
    products
    .select(
        "product_id",
        "product_category_name",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm"
    )
    .join(
        product_metrics,
        on="product_id",
        how="left"
    )
)


# ============================================================
# Handle products with no sales
# ============================================================

gold_products = (
    gold_products
    .withColumn(
        "total_orders",
        coalesce(col("total_orders"), lit(0))
    )
    .withColumn(
        "total_items_sold",
        coalesce(col("total_items_sold"), lit(0))
    )
    .withColumn(
        "unique_sellers",
        coalesce(col("unique_sellers"), lit(0))
    )
    .withColumn(
        "total_product_revenue",
        coalesce(col("total_product_revenue"), lit(0.0))
    )
    .withColumn(
        "total_freight_revenue",
        coalesce(col("total_freight_revenue"), lit(0.0))
    )
    .withColumn(
        "average_item_price",
        coalesce(col("average_item_price"), lit(0.0))
    )
)


# ============================================================
# Reorder columns
# ============================================================

gold_products = gold_products.select(
    "product_id",
    "product_category_name",
    "total_orders",
    "total_items_sold",
    "unique_sellers",
    "total_product_revenue",
    "total_freight_revenue",
    "average_item_price",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm"
)


# ============================================================
# Basic validation
# ============================================================

print("\n========== GOLD PRODUCTS VALIDATION ==========")

row_count = gold_products.count()
distinct_products = gold_products.select("product_id").distinct().count()

print(f"Row count: {row_count}")
print(f"Distinct products: {distinct_products}")

duplicate_products = (
    gold_products
    .groupBy("product_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

print(f"Duplicate product IDs: {duplicate_products}")

null_product_ids = (
    gold_products
    .filter(col("product_id").isNull())
    .count()
)

print(f"NULL product IDs: {null_product_ids}")

negative_revenue = (
    gold_products
    .filter(
        (col("total_product_revenue") < 0) |
        (col("total_freight_revenue") < 0)
    )
    .count()
)

print(f"Negative revenue records: {negative_revenue}")

invalid_metrics = (
    gold_products
    .filter(
        (col("total_orders") < 0) |
        (col("total_items_sold") < 0) |
        (col("unique_sellers") < 0) |
        (col("average_item_price") < 0)
    )
    .count()
)

print(f"Invalid metric records: {invalid_metrics}")


# ============================================================
# Show sample
# ============================================================

print("\n========== SAMPLE GOLD PRODUCTS ==========")

gold_products.show(10, truncate=False)


# ============================================================
# Write Gold Products
# ============================================================

gold_products.write \
    .mode("overwrite") \
    .parquet(GOLD_PATH)

print(f"\nGold Products written to: {GOLD_PATH}")

print("\n========== GOLD PRODUCTS COMPLETE ==========")


spark.stop()