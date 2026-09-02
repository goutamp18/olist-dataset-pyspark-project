from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("ParquetWriteTest")
    .master("local[*]")
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
    .config("spark.hadoop.fs.permissions.enabled", "false")
    .config("spark.hadoop.fs.file.impl.disable.cache", "true")
    .getOrCreate()
)

df = spark.createDataFrame(
    [
        (1, "Goutam"),
        (2, "Spark")
    ],
    ["id", "name"]
)

df.show()

df.write.mode("overwrite").parquet("data/test_parquet")

print("Parquet write successful!")

spark.stop()