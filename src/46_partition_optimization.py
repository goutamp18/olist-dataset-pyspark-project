from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum


spark = (
    SparkSession.builder
    .appName("PartitionOptimization")
    .master("local[*]")
    .config("spark.ui.port", "4040")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# 1. Read Gold Orders
# --------------------------------------------------

orders = spark.read.parquet("data/gold/orders")

print("Rows:", orders.count())
print("Original partitions:", orders.rdd.getNumPartitions())


# --------------------------------------------------
# 2. Test different partition counts
# --------------------------------------------------

partition_counts = [2, 4, 8, 16]


for num_partitions in partition_counts:

    print("\n" + "=" * 60)
    print(f"Testing {num_partitions} partitions")
    print("=" * 60)

    df = orders.repartition(num_partitions)

    print("Partitions:", df.rdd.getNumPartitions())

    result = (
        df
        .groupBy("customer_unique_id")
        .agg(
            count("order_id").alias("total_orders"),
            sum("total_order_value").alias("total_spend")
        )
    )

    print("Result rows:", result.count())


print("\n" + "=" * 60)
print("Experiment complete")
print("=" * 60)

print("\nOpen Spark UI:")
print("http://localhost:4040")

print("\nApplication will stay alive for 5 minutes...")

import time
time.sleep(300)

spark.stop()