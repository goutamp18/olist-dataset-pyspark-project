from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum,
    count,
    countDistinct,
    round,
    min,
    max
)


spark = (
    SparkSession.builder
    .appName("OlistGoldOrders")
    .master("local[*]")
    .getOrCreate()
)


SILVER_BASE_PATH = "data/silver"
GOLD_PATH = "data/gold/orders"


# ============================================================
# LOAD SILVER DATA
# ============================================================

orders = spark.read.parquet(
    f"{SILVER_BASE_PATH}/orders"
)

customers = spark.read.parquet(
    f"{SILVER_BASE_PATH}/customers"
)

order_items = spark.read.parquet(
    f"{SILVER_BASE_PATH}/order_items"
)

order_payments = spark.read.parquet(
    f"{SILVER_BASE_PATH}/order_payments"
)


# ============================================================
# AGGREGATE ORDER ITEMS
# ============================================================
#
# order_items has multiple rows per order.
# We must aggregate it to one row per order
# before joining with orders.
#
# Otherwise:
#
# orders × items × payments
#
# can multiply rows and produce incorrect revenue.
# ============================================================

items_agg = (
    order_items
    .groupBy("order_id")
    .agg(
        count("*").alias("total_items"),
        countDistinct("product_id").alias("unique_products"),
        countDistinct("seller_id").alias("unique_sellers"),
        round(sum("price"), 2).alias("product_revenue"),
        round(sum("freight_value"), 2).alias("freight_revenue")
    )
)


# ============================================================
# AGGREGATE PAYMENTS
# ============================================================
#
# An order can have multiple payment records.
# Therefore we also aggregate payments to one row per order.
# ============================================================

payments_agg = (
    order_payments
    .groupBy("order_id")
    .agg(
        count("*").alias("payment_count"),
        round(sum("payment_value"), 2).alias("total_payment_value"),
        min("payment_value").alias("minimum_payment"),
        max("payment_value").alias("maximum_payment")
    )
)


# ============================================================
# CREATE CUSTOMER LOOKUP
# ============================================================
#
# customer_id identifies a customer/order relationship.
# customer_unique_id identifies the actual customer.
# ============================================================

customer_lookup = (
    customers
    .select(
        "customer_id",
        "customer_unique_id",
        "customer_city",
        "customer_state"
    )
)


# ============================================================
# BUILD GOLD ORDER TABLE
# ============================================================

gold_orders = (
    orders

    # Add customer information
    .join(
        customer_lookup,
        on="customer_id",
        how="left"
    )

    # Add aggregated item information
    .join(
        items_agg,
        on="order_id",
        how="left"
    )

    # Add aggregated payment information
    .join(
        payments_agg,
        on="order_id",
        how="left"
    )

    # Calculate total order value
    .withColumn(
        "total_order_value",
        round(
            col("product_revenue") +
            col("freight_revenue"),
            2
        )
    )
)


# ============================================================
# INSPECT GOLD DATA
# ============================================================

print("\nGold Orders Sample:")

gold_orders.select(
    "order_id",
    "customer_unique_id",
    "order_status",
    "order_purchase_timestamp",
    "total_items",
    "unique_products",
    "unique_sellers",
    "product_revenue",
    "freight_revenue",
    "total_order_value",
    "total_payment_value",
    "payment_count"
).show(
    20,
    truncate=False
)


# ============================================================
# GOLD DATA VALIDATION
# ============================================================

print("\nGold Orders Row Count:")

gold_order_count = gold_orders.count()

print(gold_order_count)


print("\nExpected Order Count:")

expected_order_count = orders.count()

print(expected_order_count)


print("\nDuplicate Order IDs:")

duplicate_orders = (
    gold_orders
    .groupBy("order_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

print(duplicate_orders)


print("\nNULL Order IDs:")

null_order_ids = (
    gold_orders
    .filter(col("order_id").isNull())
    .count()
)

print(null_order_ids)


print("\nNULL Customer IDs:")

null_customer_ids = (
    gold_orders
    .filter(col("customer_id").isNull())
    .count()
)

print(null_customer_ids)


print("\nNegative Total Order Values:")

negative_order_values = (
    gold_orders
    .filter(col("total_order_value") < 0)
    .count()
)

print(negative_order_values)


# ============================================================
# WRITE GOLD DATA
# ============================================================

gold_orders.write \
    .mode("overwrite") \
    .parquet(GOLD_PATH)


print("\nGold orders written successfully.")


spark.stop()