from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("Cache Persist Demo")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

orders = spark.read.parquet(
    "data/gold/orders"
)

# ============================================================
# WITHOUT CACHE
# ============================================================

print("\n========== WITHOUT CACHE ==========")

delivered_orders = (
    orders
    .filter(col("order_status") == "delivered")
)

print("First action:")
print("Rows:", delivered_orders.count())

print("\nSecond action:")
print("Rows:", delivered_orders.count())


# ============================================================
# WITH CACHE
# ============================================================

print("\n========== WITH CACHE ==========")

cached_orders = (
    orders
    .filter(col("order_status") == "delivered")
    .cache()
)

print("First action:")
print("Rows:", cached_orders.count())

print("\nSecond action:")
print("Rows:", cached_orders.count())


# ============================================================
# CHECK CACHE
# ============================================================

print("\n========== CACHE STATUS ==========")

print(
    "Is cached:",
    cached_orders.is_cached
)


# ============================================================
# REMOVE CACHE
# ============================================================

cached_orders.unpersist()

print(
    "Is cached after unpersist:",
    cached_orders.is_cached
)

spark.stop()