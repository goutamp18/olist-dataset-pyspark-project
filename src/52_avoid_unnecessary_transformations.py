from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum


spark = (
    SparkSession.builder
    .appName("AvoidUnnecessaryTransformations")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

orders = spark.read.parquet("data/gold/orders")


# --------------------------------------------------
# CASE 1: Unnecessary transformations
# --------------------------------------------------

print("\n========== CASE 1: UNNECESSARY WORK ==========")

bad = (
    orders
    .select("*")
    .filter(col("order_status") == "delivered")
    .withColumn("order_value_copy", col("total_order_value"))
    .drop("order_value_copy")
    .select(
        "customer_unique_id",
        "total_order_value"
    )
    .groupBy("customer_unique_id")
    .agg(
        sum("total_order_value").alias("total_spend")
    )
)

bad.explain("formatted")


# --------------------------------------------------
# CASE 2: Cleaner version
# --------------------------------------------------

print("\n========== CASE 2: OPTIMIZED ==========")

better = (
    orders
    .filter(col("order_status") == "delivered")
    .select(
        "customer_unique_id",
        "total_order_value"
    )
    .groupBy("customer_unique_id")
    .agg(
        sum("total_order_value").alias("total_spend")
    )
)

better.explain("formatted")


# --------------------------------------------------
# Execute both
# --------------------------------------------------

print("\n========== RESULTS ==========")

print("Bad result:", bad.count())
print("Better result:", better.count())


spark.stop()