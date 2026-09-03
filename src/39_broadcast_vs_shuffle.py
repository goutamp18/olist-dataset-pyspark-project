from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

spark = (
    SparkSession.builder
    .appName("Broadcast Vs Shuffle")
    .master("local[*]")
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

orders = spark.read.parquet(
    "data/gold/orders"
)

customers = spark.read.parquet(
    "data/silver/customers"
)

# ============================================================
# NORMAL JOIN - AUTOMATIC BROADCAST DISABLED
# ============================================================

print("\n========== NORMAL JOIN ==========")

normal_join = (
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
        customers["customer_city"],
        customers["customer_state"]
    )
)

print("Rows:", normal_join.count())

print("\nExecution Plan:")
normal_join.explain()

# ============================================================
# EXPLICIT BROADCAST JOIN
# ============================================================

print("\n========== BROADCAST JOIN ==========")

broadcast_join = (
    orders
    .join(
        broadcast(customers),
        on="customer_id",
        how="left"
    )
    .select(
        orders["order_id"],
        orders["customer_id"],
        orders["total_order_value"],
        customers["customer_city"],
        customers["customer_state"]
    )
)

print("Rows:", broadcast_join.count())

print("\nExecution Plan:")
broadcast_join.explain()

spark.stop()