from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum

spark = (
    SparkSession.builder
    .appName("Explain Demo")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

orders = spark.read.parquet(
    "data/gold/orders"
)

# ============================================================
# 1. SIMPLE FILTER
# ============================================================

print("\n========== 1. FILTER ==========")

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
# 2. GROUP BY
# ============================================================

print("\n========== 2. GROUP BY ==========")

customer_metrics = (
    orders
    .groupBy("customer_unique_id")
    .agg(
        count("order_id").alias("total_orders"),
        sum("total_order_value").alias("total_spend")
    )
)

customer_metrics.explain()


# ============================================================
# 3. ORDER BY
# ============================================================

print("\n========== 3. ORDER BY ==========")

sorted_orders = (
    orders
    .orderBy(
        col("total_order_value").desc()
    )
)

sorted_orders.explain()


# ============================================================
# 4. BROADCAST JOIN
# ============================================================

print("\n========== 4. BROADCAST JOIN ==========")

customers = spark.read.parquet(
    "data/silver/customers"
)

joined_orders = (
    orders
    .join(
        customers,
        on="customer_id",
        how="left"
    )
    .select(
        orders["order_id"],
        orders["customer_id"],
        orders["total_order_value"],
        customers["customer_city"]
    )
)

joined_orders.explain()


spark.stop()