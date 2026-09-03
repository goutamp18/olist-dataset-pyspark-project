from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, min as spark_min


spark = (
    SparkSession.builder
    .appName("IncrementalProcessing")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# 1. Read Bronze Orders
# --------------------------------------------------

orders = spark.read.parquet("data/bronze/orders")

print("\n========== FULL DATASET ==========")
print("Total orders:", orders.count())

orders.select(
    spark_min("order_purchase_timestamp").alias("min_date"),
    spark_max("order_purchase_timestamp").alias("max_date")
).show(truncate=False)


# --------------------------------------------------
# 2. Simulate an incremental processing date
# --------------------------------------------------

incremental_date = "2018-08-01"

print("\nIncremental processing date:", incremental_date)


# --------------------------------------------------
# 3. Process only new orders
# --------------------------------------------------

incremental_orders = orders.filter(
    col("order_purchase_timestamp") >= incremental_date
)

print(
    "Orders to process:",
    incremental_orders.count()
)

incremental_orders.select(
    spark_min("order_purchase_timestamp").alias("min_date"),
    spark_max("order_purchase_timestamp").alias("max_date")
).show(truncate=False)


# --------------------------------------------------
# 4. Compare full vs incremental processing
# --------------------------------------------------

full_count = orders.count()
incremental_count = incremental_orders.count()

print("\n========== COMPARISON ==========")

print("Full dataset:", full_count)
print("Incremental dataset:", incremental_count)
print(
    "Orders skipped:",
    full_count - incremental_count
)


# --------------------------------------------------
# 5. Show sample incremental records
# --------------------------------------------------

print("\n========== SAMPLE INCREMENTAL DATA ==========")

(
    incremental_orders
    .select(
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp"
    )
    .orderBy("order_purchase_timestamp")
    .show(10, truncate=False)
)


spark.stop()