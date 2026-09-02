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
    .appName("Gold Sellers")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# Paths
# ============================================================

SILVER_PATH = "data/silver"
GOLD_PATH = "data/gold/sellers"


# ============================================================
# Read Silver datasets
# ============================================================

sellers = spark.read.parquet(f"{SILVER_PATH}/sellers")
order_items = spark.read.parquet(f"{SILVER_PATH}/order_items")


# ============================================================
# Aggregate seller metrics
# ============================================================

seller_metrics = (
    order_items
    .groupBy("seller_id")
    .agg(
        countDistinct("order_id").alias("total_orders"),
        count("*").alias("total_items_sold"),
        countDistinct("product_id").alias("unique_products"),
        round(sum("price"), 2).alias("total_product_revenue"),
        round(sum("freight_value"), 2).alias("total_freight_revenue"),
        round(avg("price"), 2).alias("average_item_price")
    )
)


# ============================================================
# Join seller information with metrics
# ============================================================

gold_sellers = (
    sellers
    .select(
        "seller_id",
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state"
    )
    .join(
        seller_metrics,
        on="seller_id",
        how="left"
    )
)


# ============================================================
# Handle sellers with no sales
# ============================================================

gold_sellers = (
    gold_sellers
    .withColumn(
        "total_orders",
        coalesce(col("total_orders"), lit(0))
    )
    .withColumn(
        "total_items_sold",
        coalesce(col("total_items_sold"), lit(0))
    )
    .withColumn(
        "unique_products",
        coalesce(col("unique_products"), lit(0))
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

gold_sellers = gold_sellers.select(
    "seller_id",
    "seller_zip_code_prefix",
    "seller_city",
    "seller_state",
    "total_orders",
    "total_items_sold",
    "unique_products",
    "total_product_revenue",
    "total_freight_revenue",
    "average_item_price"
)


# ============================================================
# Basic validation
# ============================================================

print("\n========== GOLD SELLERS VALIDATION ==========")

row_count = gold_sellers.count()
distinct_sellers = gold_sellers.select("seller_id").distinct().count()

print(f"Row count: {row_count}")
print(f"Distinct sellers: {distinct_sellers}")

duplicate_sellers = (
    gold_sellers
    .groupBy("seller_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

print(f"Duplicate seller IDs: {duplicate_sellers}")

null_seller_ids = (
    gold_sellers
    .filter(col("seller_id").isNull())
    .count()
)

print(f"NULL seller IDs: {null_seller_ids}")

negative_revenue = (
    gold_sellers
    .filter(
        (col("total_product_revenue") < 0) |
        (col("total_freight_revenue") < 0)
    )
    .count()
)

print(f"Negative revenue records: {negative_revenue}")

invalid_metrics = (
    gold_sellers
    .filter(
        (col("total_orders") < 0) |
        (col("total_items_sold") < 0) |
        (col("unique_products") < 0) |
        (col("average_item_price") < 0)
    )
    .count()
)

print(f"Invalid metric records: {invalid_metrics}")


# ============================================================
# Show sample
# ============================================================

print("\n========== SAMPLE GOLD SELLERS ==========")

gold_sellers.show(10, truncate=False)


# ============================================================
# Write Gold Sellers
# ============================================================

gold_sellers.write \
    .mode("overwrite") \
    .parquet(GOLD_PATH)

print(f"\nGold Sellers written to: {GOLD_PATH}")

print("\n========== GOLD SELLERS COMPLETE ==========")


spark.stop()