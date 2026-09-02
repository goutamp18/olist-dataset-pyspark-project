from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum,
    min,
    max,
    round,
    when,
    coalesce,
    lit
)


spark = (
    SparkSession.builder
    .appName("OlistGoldCustomers")
    .master("local[*]")
    .getOrCreate()
)


SILVER_BASE_PATH = "data/silver"
GOLD_ORDERS_PATH = "data/gold/orders"
GOLD_CUSTOMERS_PATH = "data/gold/customers"


# ============================================================
# LOAD DATA
# ============================================================

orders = spark.read.parquet(GOLD_ORDERS_PATH)

customers = spark.read.parquet(
    f"{SILVER_BASE_PATH}/customers"
)


# ============================================================
# HANDLE NULL ORDER METRICS
# ============================================================

orders = (
    orders
    .withColumn(
        "order_value_for_metrics",
        coalesce(
            col("total_order_value"),
            lit(0.0)
        )
    )
    .withColumn(
        "items_for_metrics",
        coalesce(
            col("total_items"),
            lit(0)
        )
    )
)


# ============================================================
# CREATE CUSTOMER-LEVEL ORDER METRICS
# ============================================================

customer_order_metrics = (
    orders
    .groupBy("customer_unique_id")
    .agg(

        # Total number of orders
        count("order_id").alias(
            "total_orders"
        ),

        # Delivered orders
        count(
            when(
                col("order_status") == "delivered",
                col("order_id")
            )
        ).alias(
            "delivered_orders"
        ),

        # Total items purchased
        sum("items_for_metrics").alias(
            "total_items"
        ),

        # Total customer spend
        round(
            sum("order_value_for_metrics"),
            2
        ).alias(
            "total_spend"
        ),

        # Average order value
        round(
            sum("order_value_for_metrics")
            / count("order_id"),
            2
        ).alias(
            "average_order_value"
        ),

        # First order
        min(
            "order_purchase_timestamp"
        ).alias(
            "first_order_date"
        ),

        # Last order
        max(
            "order_purchase_timestamp"
        ).alias(
            "last_order_date"
        )
    )
)


# ============================================================
# CREATE CUSTOMER LOOKUP
# ============================================================

customer_lookup = (
    customers
    .select(
        "customer_unique_id",
        "customer_city",
        "customer_state"
    )
    .dropDuplicates(
        ["customer_unique_id"]
    )
)


# ============================================================
# BUILD GOLD CUSTOMER DATASET
# ============================================================

gold_customers = (
    customer_order_metrics
    .join(
        customer_lookup,
        on="customer_unique_id",
        how="left"
    )
)


# ============================================================
# INSPECT GOLD CUSTOMER DATA
# ============================================================

print("\n========== GOLD CUSTOMERS SAMPLE ==========\n")

gold_customers.select(
    "customer_unique_id",
    "total_orders",
    "delivered_orders",
    "total_items",
    "total_spend",
    "average_order_value",
    "first_order_date",
    "last_order_date",
    "customer_city",
    "customer_state"
).show(
    20,
    truncate=False
)


# ============================================================
# BASIC VALIDATION
# ============================================================

print("\n========== BASIC VALIDATION ==========\n")

customer_count = gold_customers.count()

print(
    "Customer Row Count:",
    customer_count
)


distinct_customers = (
    gold_customers
    .select("customer_unique_id")
    .distinct()
    .count()
)

print(
    "Distinct Customer IDs:",
    distinct_customers
)


duplicate_customers = (
    gold_customers
    .groupBy("customer_unique_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

print(
    "Duplicate Customer IDs:",
    duplicate_customers
)


null_customer_ids = (
    gold_customers
    .filter(
        col("customer_unique_id").isNull()
    )
    .count()
)

print(
    "NULL Customer IDs:",
    null_customer_ids
)


# ============================================================
# BUSINESS VALIDATION
# ============================================================

print("\n========== BUSINESS VALIDATION ==========\n")


invalid_total_orders = (
    gold_customers
    .filter(
        col("total_orders").isNull()
        |
        (col("total_orders") < 1)
    )
    .count()
)


invalid_delivered_orders = (
    gold_customers
    .filter(
        col("delivered_orders").isNull()
        |
        (col("delivered_orders") < 0)
        |
        (
            col("delivered_orders")
            >
            col("total_orders")
        )
    )
    .count()
)


invalid_total_items = (
    gold_customers
    .filter(
        col("total_items").isNull()
        |
        (col("total_items") < 0)
    )
    .count()
)


invalid_total_spend = (
    gold_customers
    .filter(
        col("total_spend").isNull()
        |
        (col("total_spend") < 0)
    )
    .count()
)


invalid_average_order_value = (
    gold_customers
    .filter(
        col("average_order_value").isNull()
        |
        (col("average_order_value") < 0)
    )
    .count()
)


print(
    "Customers with invalid total_orders:",
    invalid_total_orders
)

print(
    "Customers with invalid delivered_orders:",
    invalid_delivered_orders
)

print(
    "Customers with invalid total_items:",
    invalid_total_items
)

print(
    "Customers with invalid total_spend:",
    invalid_total_spend
)

print(
    "Customers with invalid average_order_value:",
    invalid_average_order_value
)


# ============================================================
# WRITE GOLD DATASET
# ============================================================

print("\n========== WRITING GOLD CUSTOMERS ==========\n")

(
    gold_customers
    .write
    .mode("overwrite")
    .parquet(
        GOLD_CUSTOMERS_PATH
    )
)


print(
    "Gold Customers dataset written successfully."
)


spark.stop()