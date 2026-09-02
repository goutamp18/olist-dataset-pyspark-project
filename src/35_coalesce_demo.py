from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("Coalesce Demo")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


orders = spark.read.parquet(
    "data/gold/orders"
)


print("\n========== ORIGINAL ==========")

print("Rows:", orders.count())

print(
    "Partitions:",
    orders.rdd.getNumPartitions()
)


print("\n========== COALESCE TO 4 ==========")

orders_4 = orders.coalesce(4)

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


print("\n========== COALESCE TO 2 ==========")

orders_2 = orders.coalesce(2)

print(
    "Partitions:",
    orders_2.rdd.getNumPartitions()
)

partition_sizes_2 = (
    orders_2.rdd
    .mapPartitions(
        lambda iterator: [sum(1 for _ in iterator)]
    )
    .collect()
)

print(
    "Records in each partition:",
    partition_sizes_2
)

print(
    "Total records:",
    sum(partition_sizes_2)
)


print("\n========== COALESCE TO 12 ==========")

orders_12 = orders.coalesce(12)

print(
    "Partitions:",
    orders_12.rdd.getNumPartitions()
)

spark.stop()