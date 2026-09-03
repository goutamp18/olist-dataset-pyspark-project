from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark import StorageLevel

spark = (
    SparkSession.builder
    .appName("Persist Demo")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

orders = spark.read.parquet(
    "data/gold/orders"
)

# ============================================================
# MEMORY_ONLY
# ============================================================

print("\n========== MEMORY_ONLY ==========")

memory_only_df = (
    orders
    .filter(col("order_status") == "delivered")
    .persist(StorageLevel.MEMORY_ONLY)
)

print("Storage level before action:")
print(memory_only_df.storageLevel)

print("Rows:", memory_only_df.count())

print("Is cached:", memory_only_df.is_cached)

memory_only_df.unpersist()


# ============================================================
# MEMORY_AND_DISK
# ============================================================

print("\n========== MEMORY_AND_DISK ==========")

memory_disk_df = (
    orders
    .filter(col("order_status") == "delivered")
    .persist(StorageLevel.MEMORY_AND_DISK)
)

print("Storage level before action:")
print(memory_disk_df.storageLevel)

print("Rows:", memory_disk_df.count())

print("Is cached:", memory_disk_df.is_cached)

memory_disk_df.unpersist()


# ============================================================
# CACHE
# ============================================================

print("\n========== CACHE ==========")

cached_df = (
    orders
    .filter(col("order_status") == "delivered")
    .cache()
)

print("Storage level before action:")
print(cached_df.storageLevel)

print("Rows:", cached_df.count())

print("Is cached:", cached_df.is_cached)

cached_df.unpersist()

spark.stop()