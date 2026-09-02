from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum
from schemas.orders_schema import orders_schema


spark = (
    SparkSession.builder
    .appName("OlistOrdersValidation")
    .master("local[*]")
    .getOrCreate()
)

orders = (
    spark.read
    .option("header", True)
    .schema(orders_schema)
    .csv("data/raw/olist_orders_dataset.csv")
)


# 1. Required columns should not be NULL
print("\n========== REQUIRED FIELD CHECK ==========")

orders.filter(
    col("order_id").isNull() |
    col("customer_id").isNull()
).show()


# 2. Check invalid order IDs
print("\n========== INVALID ORDER IDs ==========")

orders.filter(
    col("order_id").isNull() |
    (col("order_id") == "")
).count()


# 3. Check invalid customer IDs
print("\n========== INVALID CUSTOMER IDs ==========")

orders.filter(
    col("customer_id").isNull() |
    (col("customer_id") == "")
).count()


# 4. Check unexpected order statuses
print("\n========== ORDER STATUS VALUES ==========")

orders.select("order_status") \
    .distinct() \
    .orderBy("order_status") \
    .show(truncate=False)


# 5. Check impossible delivery dates
print("\n========== DATE VALIDATION ==========")

invalid_dates = orders.filter(
    col("order_delivered_customer_date").isNotNull()
    &
    col("order_purchase_timestamp").isNotNull()
    &
    (
        col("order_delivered_customer_date")
        < col("order_purchase_timestamp")
    )
)

print("Invalid delivery dates:", invalid_dates.count())

invalid_dates.show(10, truncate=False)


# 6. Check approval before purchase
print("\n========== APPROVAL DATE VALIDATION ==========")

invalid_approval = orders.filter(
    col("order_approved_at").isNotNull()
    &
    col("order_purchase_timestamp").isNotNull()
    &
    (
        col("order_approved_at")
        < col("order_purchase_timestamp")
    )
)

print("Invalid approval dates:", invalid_approval.count())

invalid_approval.show(10, truncate=False)


# 7. Duplicate order IDs
print("\n========== DUPLICATE ORDER IDs ==========")

duplicates = (
    orders
    .groupBy("order_id")
    .agg(count("*").alias("record_count"))
    .filter(col("record_count") > 1)
)

print("Duplicate order IDs:", duplicates.count())


spark.stop()