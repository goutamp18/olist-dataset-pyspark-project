from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum,
    min,
    max
)


spark = (
    SparkSession.builder
    .appName("OlistOrdersProfiling")
    .master("local[*]")
    .getOrCreate()
)


# -----------------------------
# 1. Load raw data
# -----------------------------

orders = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/raw/olist_orders_dataset.csv")
)


# -----------------------------
# 2. Basic information
# -----------------------------

print("\n========== BASIC INFORMATION ==========")

print("Total rows:", orders.count())
print("Total columns:", len(orders.columns))

print("\nColumns:")
print(orders.columns)

print("\nSchema:")
orders.printSchema()


# -----------------------------
# 3. Order status distribution
# -----------------------------

print("\n========== ORDER STATUS ==========")

orders.groupBy("order_status") \
    .count() \
    .orderBy(col("count").desc()) \
    .show()


# -----------------------------
# 4. NULL values
# -----------------------------

print("\n========== NULL VALUES ==========")

null_counts = orders.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in orders.columns
])

null_counts.show()


# -----------------------------
# 5. Duplicate order IDs
# -----------------------------

print("\n========== DUPLICATE ORDER IDs ==========")

duplicate_orders = (
    orders
    .groupBy("order_id")
    .count()
    .filter(col("count") > 1)
)

duplicate_orders.show()

print(
    "Number of duplicated order IDs:",
    duplicate_orders.count()
)


# -----------------------------
# 6. Date range
# -----------------------------

print("\n========== DATE RANGE ==========")

orders.select(
    min("order_purchase_timestamp").alias("earliest_order"),
    max("order_purchase_timestamp").alias("latest_order")
).show()


# -----------------------------
# 7. Customer ID duplicates
# -----------------------------

print("\n========== CUSTOMER ID ==========")

print(
    "Unique customer_id:",
    orders.select("customer_id").distinct().count()
)

print(
    "Total orders:",
    orders.count()
)


spark.stop()