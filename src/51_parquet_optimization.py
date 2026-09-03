from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = (
    SparkSession.builder
    .appName("ParquetOptimization")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# 1. Read Parquet
# --------------------------------------------------

orders = spark.read.parquet("data/gold/orders")


# --------------------------------------------------
# 2. Check schema
# --------------------------------------------------

print("\n========== SCHEMA ==========")
orders.printSchema()


# --------------------------------------------------
# 3. Read everything
# --------------------------------------------------

print("\n========== READ ALL COLUMNS ==========")

all_columns = orders.select("*")

all_columns.explain("formatted")


# --------------------------------------------------
# 4. Read only required columns
# --------------------------------------------------

print("\n========== COLUMN PRUNING ==========")

selected_columns = (
    orders
    .select(
        "order_id",
        "customer_unique_id",
        "order_status",
        "total_order_value"
    )
)

selected_columns.explain("formatted")


# --------------------------------------------------
# 5. Predicate Pushdown
# --------------------------------------------------

print("\n========== PREDICATE PUSHDOWN ==========")

delivered_orders = (
    orders
    .filter(col("order_status") == "delivered")
    .select(
        "order_id",
        "customer_unique_id",
        "total_order_value"
    )
)

delivered_orders.explain("formatted")


# --------------------------------------------------
# 6. Execute
# --------------------------------------------------

print("\nDelivered orders:", delivered_orders.count())


spark.stop()