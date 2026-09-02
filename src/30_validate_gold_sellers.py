from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    sum,
    avg,
    round,
    abs
)

spark = (
    SparkSession.builder
    .appName("Validate Gold Sellers")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

SILVER_PATH = "data/silver"
GOLD_PATH = "data/gold/sellers"


# ============================================================
# LOAD DATA
# ============================================================

gold_sellers = spark.read.parquet(GOLD_PATH)
silver_order_items = spark.read.parquet(
    f"{SILVER_PATH}/order_items"
)


# ============================================================
# BASIC VALIDATION
# ============================================================

print("\n========== GOLD SELLERS VALIDATION ==========")

row_count = gold_sellers.count()
distinct_sellers = gold_sellers.select("seller_id").distinct().count()

duplicate_sellers = (
    gold_sellers
    .groupBy("seller_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

null_seller_ids = (
    gold_sellers
    .filter(col("seller_id").isNull())
    .count()
)

negative_revenue = (
    gold_sellers
    .filter(
        (col("total_product_revenue") < 0) |
        (col("total_freight_revenue") < 0)
    )
    .count()
)

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

print(f"Row count: {row_count}")
print(f"Distinct sellers: {distinct_sellers}")
print(f"Duplicate seller IDs: {duplicate_sellers}")
print(f"NULL seller IDs: {null_seller_ids}")
print(f"Negative revenue records: {negative_revenue}")
print(f"Invalid metric records: {invalid_metrics}")


# ============================================================
# EXPECTED ROW COUNT
# ============================================================

expected_sellers = (
    silver_order_items
    .select("seller_id")
    .distinct()
    .count()
)

print("\n========== ROW COUNT VALIDATION ==========")

print(f"Expected sellers from order_items: {expected_sellers}")
print(f"Gold sellers: {row_count}")


# ============================================================
# VALIDATE TOTAL ORDERS
# ============================================================

expected_orders = (
    silver_order_items
    .groupBy("seller_id")
    .agg(
        countDistinct("order_id").alias("expected_total_orders")
    )
)

order_mismatch = (
    gold_sellers
    .join(expected_orders, "seller_id", "left")
    .filter(
        col("total_orders") != col("expected_total_orders")
    )
    .count()
)

print(f"\nTotal orders mismatches: {order_mismatch}")


# ============================================================
# VALIDATE TOTAL ITEMS SOLD
# ============================================================

expected_items = (
    silver_order_items
    .groupBy("seller_id")
    .agg(
        count("*").alias("expected_total_items_sold")
    )
)

items_mismatch = (
    gold_sellers
    .join(expected_items, "seller_id", "left")
    .filter(
        col("total_items_sold") !=
        col("expected_total_items_sold")
    )
    .count()
)

print(f"Total items sold mismatches: {items_mismatch}")


# ============================================================
# VALIDATE UNIQUE PRODUCTS
# ============================================================

expected_products = (
    silver_order_items
    .groupBy("seller_id")
    .agg(
        countDistinct("product_id").alias("expected_unique_products")
    )
)

products_mismatch = (
    gold_sellers
    .join(expected_products, "seller_id", "left")
    .filter(
        col("unique_products") !=
        col("expected_unique_products")
    )
    .count()
)

print(f"Unique products mismatches: {products_mismatch}")


# ============================================================
# VALIDATE PRODUCT REVENUE
# ============================================================

expected_product_revenue = (
    silver_order_items
    .groupBy("seller_id")
    .agg(
        round(
            sum("price"),
            2
        ).alias("expected_product_revenue")
    )
)

revenue_mismatch = (
    gold_sellers
    .join(expected_product_revenue, "seller_id", "left")
    .filter(
        abs(
            col("total_product_revenue") -
            col("expected_product_revenue")
        ) > 0.001
    )
    .count()
)

print(f"Product revenue mismatches: {revenue_mismatch}")


# ============================================================
# VALIDATE FREIGHT REVENUE
# ============================================================

expected_freight = (
    silver_order_items
    .groupBy("seller_id")
    .agg(
        round(
            sum("freight_value"),
            2
        ).alias("expected_freight_revenue")
    )
)

freight_mismatch = (
    gold_sellers
    .join(expected_freight, "seller_id", "left")
    .filter(
        abs(
            col("total_freight_revenue") -
            col("expected_freight_revenue")
        ) > 0.001
    )
    .count()
)

print(f"Freight revenue mismatches: {freight_mismatch}")


# ============================================================
# VALIDATE AVERAGE ITEM PRICE
# ============================================================

expected_average_price = (
    silver_order_items
    .groupBy("seller_id")
    .agg(
        round(
            avg("price"),
            2
        ).alias("expected_average_item_price")
    )
)

average_price_mismatch = (
    gold_sellers
    .join(expected_average_price, "seller_id", "left")
    .filter(
        abs(
            col("average_item_price") -
            col("expected_average_item_price")
        ) > 0.001
    )
    .count()
)

print(f"Average item price mismatches: {average_price_mismatch}")


# ============================================================
# SAMPLE
# ============================================================

print("\n========== SAMPLE GOLD SELLERS ==========")

gold_sellers.show(10, truncate=False)


# ============================================================
# FINAL VALIDATION
# ============================================================

print("\n========== VALIDATION COMPLETE ==========")

spark.stop()