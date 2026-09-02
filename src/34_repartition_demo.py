from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("Repartition Demo")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# Read Gold Orders
orders = spark.read.parquet(
    "data/gold/orders"
)


# ============================================================
# ORIGINAL PARTITIONS
# ============================================================

print("\n========== ORIGINAL ==========")

print("Rows:", orders.count())

print(
    "Partitions:",
    orders.rdd.getNumPartitions()
)


# ============================================================
# REPARTITION TO 4
# ============================================================

print("\n========== REPARTITION TO 4 ==========")

orders_4 = orders.repartition(4)

print(
    "Partitions:",
    orders_4.rdd.getNumPartitions()
)

partition_sizes_4 = (
    orders_4.rdd
    .mapPartitions(
        lambda iterator: [sum(1 for _ in iterator)]
    )
    .collect()
)

print(
    "Records in each partition:",
    partition_sizes_4
)

print(
    "Total records:",
    sum(partition_sizes_4)
)


# ============================================================
# REPARTITION TO 12
# ============================================================

print("\n========== REPARTITION TO 12 ==========")

orders_12 = orders.repartition(12)

print(
    "Partitions:",
    orders_12.rdd.getNumPartitions()
)

partition_sizes_12 = (
    orders_12.rdd
    .mapPartitions(
        lambda iterator: [sum(1 for _ in iterator)]
    )
    .collect()
)

print(
    "Records in each partition:",
    partition_sizes_12
)

print(
    "Total records:",
    sum(partition_sizes_12)
)


spark.stop()