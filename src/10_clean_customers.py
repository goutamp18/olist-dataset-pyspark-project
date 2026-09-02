from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper


spark = (
    SparkSession.builder
    .appName("OlistSilverCustomers")
    .master("local[*]")
    .getOrCreate()
)


BRONZE_PATH = "data/bronze/customers"

customers = spark.read.parquet(BRONZE_PATH)


customers_clean = (
    customers
    .withColumn("customer_id", trim(col("customer_id")))
    .withColumn("customer_unique_id", trim(col("customer_unique_id")))
    .withColumn("customer_city", trim(col("customer_city")))
    .withColumn("customer_state", upper(trim(col("customer_state"))))
)


customers_clean.show(10, truncate=False)


print(
    "NULL customer_id:",
    customers_clean.filter(col("customer_id").isNull()).count()
)

print(
    "NULL customer_unique_id:",
    customers_clean.filter(col("customer_unique_id").isNull()).count()
)

print(
    "NULL customer_city:",
    customers_clean.filter(col("customer_city").isNull()).count()
)

print(
    "NULL customer_state:",
    customers_clean.filter(col("customer_state").isNull()).count()
)


SILVER_PATH = "data/silver/customers"

customers_clean.write \
    .mode("overwrite") \
    .parquet(SILVER_PATH)

print("Silver customers written successfully.")


spark.stop()
