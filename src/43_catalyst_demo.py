from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

spark = (
    SparkSession.builder
    .appName("Catalyst Optimizer Demo")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

orders = spark.read.parquet(
    "data/gold/orders"
)

# ============================================================
# QUERY
# ============================================================

result = (
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
    .filter(col("total_spend") > 1000)
)

# ============================================================
# EXPLAIN ALL PLANS
# ============================================================

print("\n========== EXPLAIN EXTENDED ==========")

result.explain(mode="extended")

spark.stop()