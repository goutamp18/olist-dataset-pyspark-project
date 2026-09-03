from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum

spark = (
    SparkSession.builder
    .appName("AQE Demo")
    .master("local[*]")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

orders = spark.read.parquet(
    "data/gold/orders"
)

# ============================================================
# AQE STATUS
# ============================================================

print("\n========== AQE CONFIGURATION ==========")

print(
    "AQE enabled:",
    spark.conf.get("spark.sql.adaptive.enabled")
)

print(
    "Shuffle partitions:",
    spark.conf.get("spark.sql.shuffle.partitions")
)

# ============================================================
# GROUP BY
# ============================================================

print("\n========== GROUP BY ==========")

customer_metrics = (
    orders
    .groupBy("customer_unique_id")
    .agg(
        count("order_id").alias("total_orders"),
        sum("total_order_value").alias("total_spend")
    )
)

print("Result rows:", customer_metrics.count())

print("\n========== EXECUTION PLAN ==========")

customer_metrics.explain(mode="formatted")

spark.stop()