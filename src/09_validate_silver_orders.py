from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("ValidateSilverOrders")
    .master("local[*]")
    .getOrCreate()
)


SILVER_PATH = "data/silver/orders"

orders = spark.read.parquet(SILVER_PATH)


print("Silver Orders Count:", orders.count())

print("\nSilver Orders Schema:")
orders.printSchema()

print("\nSilver Orders Sample:")
orders.select(
    "order_id",
    "order_status",
    "delivery_days",
    "is_delivered"
).show(10, truncate=False)


spark.stop()
