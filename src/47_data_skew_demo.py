import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count


spark = (
    SparkSession.builder
    .appName("DataSkewDemo")
    .master("local[*]")
    .config("spark.ui.port", "4040")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# 1. Read Gold Orders
# --------------------------------------------------

orders = spark.read.parquet("data/gold/orders")

print("Total rows:", orders.count())


# --------------------------------------------------
# 2. Create intentionally skewed data
# --------------------------------------------------

skewed_orders = orders.withColumn(
    "skewed_key",
    col("order_status")
)

print("\nSkewed key distribution:")
skewed_orders.groupBy("skewed_key").count().show()


# --------------------------------------------------
# 3. Repartition using the skewed key
# --------------------------------------------------

skewed_partitioned = (
    skewed_orders
    .repartition(8, "skewed_key")
)

print(
    "Partitions after repartition:",
    skewed_partitioned.rdd.getNumPartitions()
)


# --------------------------------------------------
# 4. Inspect partition sizes
# --------------------------------------------------

partition_sizes = (
    skewed_partitioned
    .rdd
    .mapPartitions(lambda rows: [sum(1 for _ in rows)])
    .collect()
)

print("\nPartition sizes:")
for index, size in enumerate(partition_sizes):
    print(f"Partition {index}: {size} rows")


# --------------------------------------------------
# 5. Perform aggregation
# --------------------------------------------------

result = (
    skewed_partitioned
    .groupBy("skewed_key")
    .agg(
        count("order_id").alias("order_count")
    )
)

print("\nAggregation result:")
result.show()


# --------------------------------------------------
# Keep Spark UI alive
# --------------------------------------------------

print("\nSpark UI:")
print("http://localhost:4040")

print("\nApplication will stay alive for 5 minutes...")

time.sleep(300)

spark.stop()