from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Spark Partitions Demo")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# LOAD EXISTING GOLD DATA
# ============================================================

orders = spark.read.parquet(
    "data/gold/orders"
)


# ============================================================
# BASIC INFORMATION
# ============================================================

print("\n========== DATASET INFORMATION ==========")

print(f"Rows: {orders.count()}")
print(f"Columns: {len(orders.columns)}")


# ============================================================
# NUMBER OF PARTITIONS
# ============================================================

print("\n========== PARTITION INFORMATION ==========")

print(
    "Number of partitions:",
    orders.rdd.getNumPartitions()
)


# ============================================================
# PARTITION DISTRIBUTION
# ============================================================

partition_sizes = (
    orders.rdd
    .mapPartitions(
        lambda iterator: [sum(1 for _ in iterator)]
    )
    .collect()
)

print(
    "Records in each partition:",
    partition_sizes
)


# ============================================================
# TOTAL RECORDS FROM PARTITIONS
# ============================================================

print(
    "Total records from partitions:",
    sum(partition_sizes)
)


# ============================================================
# STOP SPARK
# ============================================================

spark.stop()