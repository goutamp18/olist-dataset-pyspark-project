from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

spark = (
    SparkSession.builder
    .appName("ShuffleOptimization")
    .master("local[*]")
    .config("spark.ui.port", "4040")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

orders = spark.read.parquet("data/gold/orders")


# ============================================================
# CASE 1: Unnecessary repartition
# ============================================================

print("\n" + "=" * 70)
print("CASE 1: Unnecessary repartition")
print("=" * 70)

bad = (
    orders
    .repartition(20)
    .filter(col("order_status") == "delivered")
    .select(
        "order_id",
        "customer_unique_id",
        "total_order_value"
    )
)

print("Result:")
bad.count()

print("\nExecution Plan:")
bad.explain(mode="formatted")


# ============================================================
# CASE 2: Filter first, repartition later
# ============================================================

print("\n" + "=" * 70)
print("CASE 2: Filter before repartition")
print("=" * 70)

better = (
    orders
    .filter(col("order_status") == "delivered")
    .select(
        "order_id",
        "customer_unique_id",
        "total_order_value"
    )
    .repartition(20)
)

print("Result:")
better.count()

print("\nExecution Plan:")
better.explain(mode="formatted")


# ============================================================
# CASE 3: Aggregation
# ============================================================

print("\n" + "=" * 70)
print("CASE 3: GroupBy shuffle")
print("=" * 70)

aggregated = (
    orders
    .filter(col("order_status") == "delivered")
    .groupBy("customer_unique_id")
    .agg(
        sum("total_order_value").alias("total_spend")
    )
)

print("Result:")
aggregated.count()

print("\nExecution Plan:")
aggregated.explain(mode="formatted")


# ============================================================
# Keep Spark UI alive
# ============================================================

print("\n" + "=" * 70)
print("Experiment complete")
print("=" * 70)

print("\nSpark UI:")
print("http://localhost:4040")

import time

print("\nApplication will stay alive for 5 minutes...")

time.sleep(300)

spark.stop()