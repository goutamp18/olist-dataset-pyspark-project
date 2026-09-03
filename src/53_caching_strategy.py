from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum


spark = (
    SparkSession.builder
    .appName("CachingStrategy")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


orders = spark.read.parquet("data/gold/orders")


# --------------------------------------------------
# CASE 1: No cache
# --------------------------------------------------

print("\n========== CASE 1: NO CACHE ==========")

delivered = orders.filter(
    col("order_status") == "delivered"
)

print("First action:", delivered.count())
print("Second action:", delivered.count())


# --------------------------------------------------
# CASE 2: Cache
# --------------------------------------------------

print("\n========== CASE 2: WITH CACHE ==========")

delivered_cached = (
    orders
    .filter(col("order_status") == "delivered")
    .cache()
)

print("Before action - cached:", delivered_cached.is_cached)

print("First action:", delivered_cached.count())

print("After first action - cached:",
      delivered_cached.is_cached)

print("Second action:", delivered_cached.count())


# --------------------------------------------------
# CASE 3: Cache a DataFrame used only once
# --------------------------------------------------

print("\n========== CASE 3: UNNECESSARY CACHE ==========")

single_use = (
    orders
    .filter(col("order_status") == "delivered")
    .groupBy("customer_unique_id")
    .agg(
        sum("total_order_value").alias("total_spend")
    )
    .cache()
)

print("Result:", single_use.count())

print(
    "Cached even though used only once:",
    single_use.is_cached
)


# --------------------------------------------------
# Clean up
# --------------------------------------------------

delivered_cached.unpersist()
single_use.unpersist()

print("\nCaches cleared.")


spark.stop()