from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count


spark = (
    SparkSession.builder
    .appName("Shuffle Demo")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


orders = spark.read.parquet(
    "data/gold/orders"
)


# ============================================================
# ORIGINAL DATA
# ============================================================

print("\n========== ORIGINAL DATA ==========")

print("Rows:", orders.count())

print(
    "Partitions:",
    orders.rdd.getNumPartitions()
)


# ============================================================
# SIMPLE FILTER
# ============================================================

print("\n========== FILTER ==========")

filtered_orders = (
    orders
    .filter(col("order_status") == "delivered")
)

print(
    "Filtered rows:",
    filtered_orders.count()
)

print(
    "Partitions:",
    filtered_orders.rdd.getNumPartitions()
)

print("\nExecution Plan:")

filtered_orders.explain()


# ============================================================
# GROUP BY
# ============================================================

print("\n========== GROUP BY ==========")

customer_orders = (
    orders
    .groupBy("customer_unique_id")
    .agg(
        count("order_id").alias("total_orders"),
        sum("total_order_value").alias("total_spend")
    )
)

print(
    "Result rows:",
    customer_orders.count()
)

print(
    "Partitions:",
    customer_orders.rdd.getNumPartitions()
)

print("\nExecution Plan:")

customer_orders.explain()


# ============================================================
# REPARTITION
# ============================================================

print("\n========== REPARTITION ==========")

repartitioned_orders = orders.repartition(4)

print(
    "Partitions:",
    repartitioned_orders.rdd.getNumPartitions()
)

print("\nExecution Plan:")

repartitioned_orders.explain()


spark.stop()