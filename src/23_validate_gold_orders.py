from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum,
    count,
    countDistinct,
    round
)


spark = (
    SparkSession.builder
    .appName("OlistValidateGoldOrders")
    .master("local[*]")
    .getOrCreate()
)


GOLD_PATH = "data/gold/orders"


# ============================================================
# LOAD GOLD ORDERS
# ============================================================

gold_orders = spark.read.parquet(GOLD_PATH)


print("\n" + "=" * 70)
print("GOLD ORDERS VALIDATION")
print("=" * 70)


# ============================================================
# BASIC VALIDATION
# ============================================================

row_count = gold_orders.count()

distinct_order_ids = (
    gold_orders
    .select("order_id")
    .distinct()
    .count()
)

duplicate_order_ids = row_count - distinct_order_ids

null_order_ids = (
    gold_orders
    .filter(col("order_id").isNull())
    .count()
)

null_customer_ids = (
    gold_orders
    .filter(col("customer_id").isNull())
    .count()
)


print("\nBasic Validation:")
print("Row count:", row_count)
print("Distinct order IDs:", distinct_order_ids)
print("Duplicate order IDs:", duplicate_order_ids)
print("NULL order IDs:", null_order_ids)
print("NULL customer IDs:", null_customer_ids)


# ============================================================
# REVENUE VALIDATION
# ============================================================

print("\nRevenue Validation:")


invalid_total_values = (
    gold_orders
    .filter(
        col("total_order_value") !=
        round(
            col("product_revenue") +
            col("freight_revenue"),
            2
        )
    )
    .count()
)

print(
    "Incorrect total_order_value calculations:",
    invalid_total_values
)


negative_product_revenue = (
    gold_orders
    .filter(col("product_revenue") < 0)
    .count()
)

negative_freight_revenue = (
    gold_orders
    .filter(col("freight_revenue") < 0)
    .count()
)

negative_total_order_value = (
    gold_orders
    .filter(col("total_order_value") < 0)
    .count()
)


print(
    "Negative product revenue:",
    negative_product_revenue
)

print(
    "Negative freight revenue:",
    negative_freight_revenue
)

print(
    "Negative total order value:",
    negative_total_order_value
)


# ============================================================
# ITEM VALIDATION
# ============================================================

print("\nItem Validation:")


invalid_total_items = (
    gold_orders
    .filter(col("total_items") <= 0)
    .count()
)

invalid_unique_products = (
    gold_orders
    .filter(col("unique_products") <= 0)
    .count()
)

invalid_unique_sellers = (
    gold_orders
    .filter(col("unique_sellers") <= 0)
    .count()
)


print(
    "Orders with invalid total_items:",
    invalid_total_items
)

print(
    "Orders with invalid unique_products:",
    invalid_unique_products
)

print(
    "Orders with invalid unique_sellers:",
    invalid_unique_sellers
)


# ============================================================
# PAYMENT VALIDATION
# ============================================================

print("\nPayment Validation:")


negative_payment_values = (
    gold_orders
    .filter(col("total_payment_value") < 0)
    .count()
)

print(
    "Orders with negative total payment:",
    negative_payment_values
)


# ============================================================
# ORDER STATUS DISTRIBUTION
# ============================================================

print("\nOrder Status Distribution:")

gold_orders.groupBy(
    "order_status"
).count().orderBy(
    col("count").desc()
).show(
    truncate=False
)


# ============================================================
# REVENUE SUMMARY
# ============================================================

print("\nRevenue Summary:")

gold_orders.select(
    round(sum("product_revenue"), 2).alias("total_product_revenue"),
    round(sum("freight_revenue"), 2).alias("total_freight_revenue"),
    round(sum("total_order_value"), 2).alias("total_order_value"),
    round(sum("total_payment_value"), 2).alias("total_payment_value")
).show(
    truncate=False
)


# ============================================================
# CUSTOMER SUMMARY
# ============================================================

print("\nCustomer Summary:")

unique_customers = (
    gold_orders
    .select("customer_unique_id")
    .distinct()
    .count()
)

print(
    "Unique customers:",
    unique_customers
)


# ============================================================
# FINAL RESULT
# ============================================================

print("\n" + "=" * 70)
print("GOLD ORDERS VALIDATION COMPLETE")
print("=" * 70)

print(
    "\nGold Orders dataset successfully loaded and validated."
)


spark.stop()