from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count


spark = (
    SparkSession.builder
    .appName("Narrow Wide Demo")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


orders = spark.read.parquet(
    "data/gold/orders"
)


# ============================================================
# NARROW TRANSFORMATION
# ============================================================

print("\n========== NARROW TRANSFORMATION ==========")

filtered_orders = (
    orders
    .filter(col("order_status") == "delivered")
    .select(
        "order_id",
        "customer_unique_id",
        "total_order_value"
    )
)

filtered_orders.explain()


# ============================================================
# WIDE TRANSFORMATION
# ============================================================

print("\n========== WIDE TRANSFORMATION ==========")

customer_orders = (
    orders
    .groupBy("customer_unique_id")
    .agg(
        count("order_id").alias("total_orders")
    )
)

customer_orders.explain()


# ============================================================
# ANOTHER WIDE TRANSFORMATION
# ============================================================

print("\n========== ORDER BY ==========")

sorted_orders = (
    orders
    .orderBy(col("total_order_value").desc())
)

sorted_orders.explain()


spark.stop()