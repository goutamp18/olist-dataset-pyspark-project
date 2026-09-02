from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("CheckGoldCustomerFiles")
    .master("local[*]")
    .getOrCreate()
)

GOLD_PATH = "data/gold/customers"

customers = spark.read.parquet(GOLD_PATH)

print("\n========== GOLD CUSTOMERS FILE CHECK ==========\n")

# --------------------------------------------------
# 1. Basic counts
# --------------------------------------------------

print("Row count:", customers.count())

print(
    "Distinct customer IDs:",
    customers.select("customer_unique_id").distinct().count()
)


# --------------------------------------------------
# 2. NULL metric check
# --------------------------------------------------

print(
    "NULL total_items:",
    customers.filter(col("total_items").isNull()).count()
)

print(
    "NULL total_spend:",
    customers.filter(col("total_spend").isNull()).count()
)

print(
    "NULL average_order_value:",
    customers.filter(
        col("average_order_value").isNull()
    ).count()
)


# --------------------------------------------------
# 3. Problem customer
# --------------------------------------------------

print("\n========== PROBLEM CUSTOMER ==========\n")

customers.filter(
    col("customer_unique_id") == "0bbb42a9ca8179ccfce14a11d2afe6de"
).show(
    truncate=False
)


# --------------------------------------------------
# 4. Another problem customer
# --------------------------------------------------

print("\n========== SECOND PROBLEM CUSTOMER ==========\n")

customers.filter(
    col("customer_unique_id") == "df19c809342ecae8391e68dccf16fc8f"
).show(
    truncate=False
)


# --------------------------------------------------
# 5. Explain Parquet files
# --------------------------------------------------

print("\n========== DATA SOURCE ==========\n")

customers.explain(True)


# --------------------------------------------------
# 6. Schema
# --------------------------------------------------

print("\n========== SCHEMA ==========\n")

customers.printSchema()


# --------------------------------------------------
# 7. Sample metrics
# --------------------------------------------------

print("\n========== SAMPLE METRICS ==========\n")

customers.select(
    "customer_unique_id",
    "total_orders",
    "total_items",
    "total_spend",
    "average_order_value"
).show(
    20,
    truncate=False
)


spark.stop()