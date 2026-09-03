from datetime import timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    max as spark_max,
    row_number,
)
from pyspark.sql.window import Window


spark = (
    SparkSession.builder
    .appName("LateArrivingData")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# 1. Configuration
# --------------------------------------------------

orders_path = "data/bronze/orders"

last_checkpoint = "2018-10-01 00:00:00"

lookback_hours = 24


# --------------------------------------------------
# 2. Read source
# --------------------------------------------------

orders = spark.read.parquet(orders_path)


# --------------------------------------------------
# 3. Calculate lookback window
# --------------------------------------------------

checkpoint_timestamp = (
    spark
    .createDataFrame(
        [(last_checkpoint,)],
        ["checkpoint"]
    )
    .select(
        col("checkpoint").cast("timestamp")
    )
    .collect()[0]["checkpoint"]
)

window_start = checkpoint_timestamp - timedelta(
    hours=lookback_hours
)

print("\n========== CHECKPOINT ==========")
print("Last checkpoint:", checkpoint_timestamp)

print("\n========== LOOKBACK ==========")
print("Lookback hours:", lookback_hours)
print("Processing from:", window_start)


# --------------------------------------------------
# 4. Read data using lookback window
# --------------------------------------------------

incremental_orders = orders.filter(
    col("order_purchase_timestamp") >= lit(window_start)
)


print("\nOrders inside lookback window:")
print(incremental_orders.count())


# --------------------------------------------------
# 5. Simulate duplicate records
# --------------------------------------------------

duplicated_orders = incremental_orders.unionByName(
    incremental_orders.limit(100)
)

print("\nRows after simulated duplicates:")
print(duplicated_orders.count())


# --------------------------------------------------
# 6. Deduplicate
# --------------------------------------------------

window_spec = (
    Window
    .partitionBy("order_id")
    .orderBy(
        col("order_purchase_timestamp").desc()
    )
)

deduplicated_orders = (
    duplicated_orders
    .withColumn(
        "row_number",
        row_number().over(window_spec)
    )
    .filter(col("row_number") == 1)
    .drop("row_number")
)


print("\nRows after deduplication:")
print(deduplicated_orders.count())


# --------------------------------------------------
# 7. Find new checkpoint
# --------------------------------------------------

new_checkpoint = (
    deduplicated_orders
    .select(
        spark_max("order_purchase_timestamp")
        .alias("max_timestamp")
    )
    .collect()[0]["max_timestamp"]
)


print("\n========== NEW CHECKPOINT ==========")
print(new_checkpoint)


# --------------------------------------------------
# 8. Summary
# --------------------------------------------------

print("\n========== SUMMARY ==========")

print("Previous checkpoint:", checkpoint_timestamp)
print("Lookback start:", window_start)
print("Rows before deduplication:", duplicated_orders.count())
print("Rows after deduplication:", deduplicated_orders.count())
print("New checkpoint:", new_checkpoint)


spark.stop()