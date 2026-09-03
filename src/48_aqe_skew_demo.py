import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, count


spark = (
    SparkSession.builder
    .appName("AQESkewDemo")
    .master("local[*]")
    .config("spark.ui.port", "4040")
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "2")
    .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "1")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("AQE enabled:",
      spark.conf.get("spark.sql.adaptive.enabled"))

print("AQE skew join enabled:",
      spark.conf.get("spark.sql.adaptive.skewJoin.enabled"))

print("Broadcast disabled:",
      spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))


# --------------------------------------------------
# 1. Read real customer/order data
# --------------------------------------------------

orders = spark.read.parquet("data/gold/orders")

customers = spark.read.parquet("data/silver/customers")


# --------------------------------------------------
# 2. Create a deliberately skewed orders dataset
# --------------------------------------------------

normal_orders = (
    orders
    .select("order_id", "customer_id")
)

skewed_orders = (
    normal_orders
    .filter(col("customer_id").isNotNull())
    .unionByName(
        normal_orders
        .filter(col("customer_id").isNotNull())
        .limit(1)
        .withColumn("order_id", lit("SKEWED_ORDER"))
        .crossJoin(
            spark.range(100000)
            .select(
                col("id").cast("string").alias("_dummy")
            )
        )
        .drop("_dummy")
    )
)

print("\nNormal orders:", normal_orders.count())
print("Skewed orders:", skewed_orders.count())


# --------------------------------------------------
# 3. Check the artificial skew
# --------------------------------------------------

print("\nTop customer IDs by order count:")

(
    skewed_orders
    .groupBy("customer_id")
    .count()
    .orderBy(col("count").desc())
    .show(10)
)


# --------------------------------------------------
# 4. Repartition by customer_id
# --------------------------------------------------

skewed_orders = skewed_orders.repartition(
    8,
    "customer_id"
)

customers = customers.repartition(
    8,
    "customer_id"
)


# --------------------------------------------------
# 5. Perform shuffle join
# --------------------------------------------------

print("\nExecuting skewed join...")

result = (
    skewed_orders
    .join(
        customers,
        "customer_id",
        "inner"
    )
)

print("Join result rows:", result.count())


# --------------------------------------------------
# 6. Execution plan
# --------------------------------------------------

print("\nExecution Plan:")

result.explain(mode="formatted")


# --------------------------------------------------
# Keep application alive
# --------------------------------------------------

print("\nSpark UI:")
print("http://localhost:4040")

print("\nApplication will stay alive for 5 minutes...")

time.sleep(300)

spark.stop()