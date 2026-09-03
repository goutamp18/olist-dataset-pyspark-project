from pyspark.sql import SparkSession

from config import (
    ENV,
    BRONZE_DIR,
    GOLD_DIR,
    SPARK_MASTER
)


# --------------------------------------------------
# Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .master(SPARK_MASTER)
    .appName("ConfigurationDemo")
    .getOrCreate()
)


# --------------------------------------------------
# Configuration
# --------------------------------------------------

print("Environment:", ENV)
print("Bronze Directory:", BRONZE_DIR)
print("Gold Directory:", GOLD_DIR)
print("Spark Master:", SPARK_MASTER)


# --------------------------------------------------
# Read Bronze data using configuration
# --------------------------------------------------

orders_path = BRONZE_DIR / "orders"

orders = spark.read.parquet(str(orders_path))

print("Orders:", orders.count())


# --------------------------------------------------
# Stop Spark
# --------------------------------------------------

spark.stop()