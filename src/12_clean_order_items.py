from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, round


spark = (
    SparkSession.builder
    .appName("OlistSilverOrderItems")
    .master("local[*]")
    .getOrCreate()
)


BRONZE_PATH = "data/bronze/order_items"

order_items = spark.read.parquet(BRONZE_PATH)


order_items_clean = (
    order_items
    .withColumn("order_id", trim(col("order_id")))
    .withColumn("product_id", trim(col("product_id")))
    .withColumn("seller_id", trim(col("seller_id")))
    .withColumn("price", round(col("price"), 2))
    .withColumn("freight_value", round(col("freight_value"), 2))
)


order_items_clean.show(10, truncate=False)


print(
    "NULL order_id:",
    order_items_clean.filter(col("order_id").isNull()).count()
)

print(
    "NULL product_id:",
    order_items_clean.filter(col("product_id").isNull()).count()
)

print(
    "NULL seller_id:",
    order_items_clean.filter(col("seller_id").isNull()).count()
)

print(
    "NULL price:",
    order_items_clean.filter(col("price").isNull()).count()
)

print(
    "NULL freight_value:",
    order_items_clean.filter(col("freight_value").isNull()).count()
)


invalid_price = order_items_clean.filter(col("price") < 0)

print("Invalid price:", invalid_price.count())


invalid_freight = order_items_clean.filter(col("freight_value") < 0)

print("Invalid freight value:", invalid_freight.count())


SILVER_PATH = "data/silver/order_items"

order_items_clean.write \
    .mode("overwrite") \
    .parquet(SILVER_PATH)

print("Silver order_items written successfully.")


spark.stop()