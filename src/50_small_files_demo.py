from pathlib import Path
import shutil

from pyspark.sql import SparkSession


# --------------------------------------------------
# 1. Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("SmallFilesDemo")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# 2. Read Gold Orders
# --------------------------------------------------

orders = spark.read.parquet("data/gold/orders")

print("Original partitions:", orders.rdd.getNumPartitions())
print("Total rows:", orders.count())


# --------------------------------------------------
# 3. Create output directories
# --------------------------------------------------

small_files_path = Path("data/tmp/small_files")
optimized_files_path = Path("data/tmp/optimized_files")

for path in [small_files_path, optimized_files_path]:
    if path.exists():
        shutil.rmtree(path)


# --------------------------------------------------
# 4. BAD: Create many small files
# --------------------------------------------------

small_files_df = orders.repartition(50)

print("\nSmall-files DataFrame partitions:",
      small_files_df.rdd.getNumPartitions())

small_files_df.write.mode("overwrite").parquet(
    str(small_files_path)
)


# Count Parquet data files
small_file_count = len(
    list(small_files_path.glob("part-*.parquet"))
)

print("Small-files output files:", small_file_count)


# --------------------------------------------------
# 5. BETTER: Reduce output partitions
# --------------------------------------------------

optimized_df = orders.coalesce(4)

print("\nOptimized DataFrame partitions:",
      optimized_df.rdd.getNumPartitions())

optimized_df.write.mode("overwrite").parquet(
    str(optimized_files_path)
)


optimized_file_count = len(
    list(optimized_files_path.glob("part-*.parquet"))
)

print("Optimized output files:", optimized_file_count)


# --------------------------------------------------
# 6. Compare
# --------------------------------------------------

print("\n========== COMPARISON ==========")

print("Original partitions :", orders.rdd.getNumPartitions())
print("Small-files files   :", small_file_count)
print("Optimized files     :", optimized_file_count)

print("\nLesson:")
print("- More output partitions -> more files")
print("- Too many tiny files -> small files problem")
print("- coalesce() can reduce output file count")
print("- repartition() performs a shuffle")
print("- coalesce() usually avoids a full shuffle when reducing partitions")


spark.stop()