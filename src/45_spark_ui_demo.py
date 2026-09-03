import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum


spark = (
    SparkSession.builder
    .appName("SparkUIDemo")
    .master("local[*]")
    .config("spark.ui.port", "4040")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Application ID:", spark.sparkContext.applicationId)
print("Spark UI:", spark.sparkContext.uiWebUrl)


# --------------------------------------------------
# 1. Read Gold Orders
# --------------------------------------------------

orders = spark.read.parquet("data/gold/orders")

print("Orders:", orders.count())


# --------------------------------------------------
# 2. Filter operation
# --------------------------------------------------

delivered = orders.filter(
    col("order_status") == "delivered"
)

print("Delivered orders:", delivered.count())


# --------------------------------------------------
# 3. Aggregation
# --------------------------------------------------

customer_metrics = (
    orders
    .groupBy("customer_unique_id")
    .agg(
        count("order_id").alias("total_orders"),
        sum("total_order_value").alias("total_spend")
    )
)

print("Customer metrics:", customer_metrics.count())


# --------------------------------------------------
# 4. Sorting
# --------------------------------------------------

top_orders = (
    orders
    .orderBy(col("total_order_value").desc())
    .select(
        "order_id",
        "customer_unique_id",
        "total_order_value"
    )
    .limit(10)
)

print("Top orders:")
top_orders.show()


# --------------------------------------------------
# Keep Spark application alive
# --------------------------------------------------

print("\nSpark UI is available at:")
print("http://localhost:4040")

print("\nApplication will stay alive for 50 minutes...")
time.sleep(3000)


spark.stop()