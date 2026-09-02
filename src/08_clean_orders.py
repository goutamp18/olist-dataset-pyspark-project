from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, datediff


spark = (
    SparkSession.builder
    .appName("OlistSilverOrders")
    .master("local[*]")
    .getOrCreate()
)


BRONZE_PATH = "data/bronze/orders"

orders = spark.read.parquet(BRONZE_PATH)


orders_clean = (
    orders
    .withColumn("order_id", trim(col("order_id")))
    .withColumn("customer_id", trim(col("customer_id")))
    .withColumn("order_status", lower(trim(col("order_status"))))
    .withColumn(
        "delivery_days",
        datediff(
            col("order_delivered_customer_date"),
            col("order_purchase_timestamp")
        )
    )
    .withColumn(
        "is_delivered",
        col("order_status") == "delivered"
    )
)


orders_clean.select(
    "order_id",
    "order_status",
    "order_purchase_timestamp",
    "order_delivered_customer_date",
    "delivery_days",
    "is_delivered"
).show(10, truncate=False)


invalid_delivery_days = orders_clean.filter(
    col("delivery_days") < 0
)

print("Invalid delivery days:", invalid_delivery_days.count())

invalid_delivery_days.show(truncate=False)


print(
    "NULL order_id:",
    orders_clean.filter(col("order_id").isNull()).count()
)

print(
    "NULL customer_id:",
    orders_clean.filter(col("customer_id").isNull()).count()
)

print(
    "NULL order_status:",
    orders_clean.filter(col("order_status").isNull()).count()
)


SILVER_PATH = "data/silver/orders"

orders_clean.write \
    .mode("overwrite") \
    .parquet(SILVER_PATH)

print("Silver orders written successfully.")


spark.stop()