from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, abs

spark = (
    SparkSession.builder
    .appName("ValidateGoldCustomers")
    .master("local[*]")
    .getOrCreate()
)

GOLD_PATH = "data/gold/customers"

customers = spark.read.parquet(GOLD_PATH)

print("\n========== GOLD CUSTOMERS VALIDATION ==========\n")


# --------------------------------------------------
# 1. Row Count
# --------------------------------------------------

row_count = customers.count()

print("Row count:", row_count)
print("Expected row count: 96096")

if row_count == 96096:
    print("PASS: Row count is correct.")
else:
    print("FAIL: Row count mismatch.")


# --------------------------------------------------
# 2. Distinct Customer IDs
# --------------------------------------------------

distinct_customers = (
    customers
    .select("customer_unique_id")
    .distinct()
    .count()
)

print("\nDistinct customer IDs:", distinct_customers)

if distinct_customers == row_count:
    print("PASS: One row per customer.")
else:
    print("FAIL: Duplicate customer IDs exist.")


# --------------------------------------------------
# 3. Duplicate Customer IDs
# --------------------------------------------------

duplicate_customers = (
    customers
    .groupBy("customer_unique_id")
    .count()
    .filter(col("count") > 1)
)

duplicate_count = duplicate_customers.count()

print("\nDuplicate customer IDs:", duplicate_count)

if duplicate_count == 0:
    print("PASS: No duplicate customer IDs.")
else:
    print("FAIL: Duplicate customer IDs found.")
    duplicate_customers.show(truncate=False)


# --------------------------------------------------
# 4. NULL Customer IDs
# --------------------------------------------------

null_customer_ids = (
    customers
    .filter(col("customer_unique_id").isNull())
    .count()
)

print("\nNULL customer IDs:", null_customer_ids)

if null_customer_ids == 0:
    print("PASS: No NULL customer IDs.")
else:
    print("FAIL: NULL customer IDs found.")


# --------------------------------------------------
# 5. Validate Total Orders
# --------------------------------------------------

invalid_total_orders = (
    customers
    .filter(
        col("total_orders").isNull() |
        (col("total_orders") < 1)
    )
    .count()
)

print(
    "\nCustomers with invalid total_orders:",
    invalid_total_orders
)

if invalid_total_orders == 0:
    print("PASS: total_orders values are valid.")
else:
    print("FAIL: Invalid total_orders found.")


# --------------------------------------------------
# 6. Delivered Orders <= Total Orders
# --------------------------------------------------

invalid_delivered_orders = (
    customers
    .filter(
        col("delivered_orders").isNull() |
        (col("delivered_orders") < 0) |
        (col("delivered_orders") > col("total_orders"))
    )
    .count()
)

print(
    "\nCustomers with invalid delivered_orders:",
    invalid_delivered_orders
)

if invalid_delivered_orders == 0:
    print("PASS: delivered_orders values are valid.")
else:
    print("FAIL: Invalid delivered_orders found.")


# --------------------------------------------------
# 7. Validate Total Items
# --------------------------------------------------

invalid_total_items = (
    customers
    .filter(
        col("total_items").isNull() |
        (col("total_items") < 0)
    )
    .count()
)

print("\nCustomers with invalid total_items:", invalid_total_items)

if invalid_total_items == 0:
    print("PASS: total_items values are valid.")
else:
    print("FAIL: Invalid total_items found.")


# --------------------------------------------------
# 8. Validate Total Spend
# --------------------------------------------------

invalid_total_spend = (
    customers
    .filter(
        col("total_spend").isNull() |
        (col("total_spend") < 0)
    )
    .count()
)

print("\nCustomers with invalid total_spend:", invalid_total_spend)

if invalid_total_spend == 0:
    print("PASS: total_spend values are valid.")
else:
    print("FAIL: Invalid total_spend found.")


# --------------------------------------------------
# 9. Validate Average Order Value
# --------------------------------------------------

invalid_average_order_value = (
    customers
    .filter(
        col("average_order_value").isNull() |
        (col("average_order_value") < 0)
    )
    .count()
)

print(
    "\nCustomers with invalid average_order_value:",
    invalid_average_order_value
)

if invalid_average_order_value == 0:
    print("PASS: average_order_value values are valid.")
else:
    print("FAIL: Invalid average_order_value found.")


# --------------------------------------------------
# 10. Validate First and Last Order Dates
# --------------------------------------------------

invalid_order_dates = (
    customers
    .filter(
        col("first_order_date").isNull() |
        col("last_order_date").isNull() |
        (col("first_order_date") > col("last_order_date"))
    )
    .count()
)

print("\nCustomers with invalid order dates:", invalid_order_dates)

if invalid_order_dates == 0:
    print("PASS: Order date ranges are valid.")
else:
    print("FAIL: Invalid order date ranges found.")


# --------------------------------------------------
# 11. Validate Average Order Value Calculation
# --------------------------------------------------

customers_with_difference = (
    customers
    .withColumn(
        "calculated_average_order_value",
        round(
            col("total_spend") / col("total_orders"),
            2
        )
    )
    .withColumn(
        "average_difference",
        round(
            col("average_order_value")
            - col("calculated_average_order_value"),
            2
        )
    )
    .filter(
    abs(col("average_difference")) > 0.01
)
)

incorrect_average_order_value = (
    customers_with_difference.count()
)

print(
    "\nCustomers with incorrect average_order_value:",
    incorrect_average_order_value
)

if incorrect_average_order_value == 0:
    print(
        "PASS: average_order_value calculation is correct."
    )
else:
    print(
        "FAIL: average_order_value calculation mismatch."
    )

    print("\nExamples of differences:")

    customers_with_difference.select(
        "customer_unique_id",
        "total_orders",
        "total_spend",
        "average_order_value",
        "calculated_average_order_value",
        "average_difference"
    ).show(20, truncate=False)


# --------------------------------------------------
# 12. Customer Location NULL Check
# --------------------------------------------------

null_customer_city = (
    customers
    .filter(col("customer_city").isNull())
    .count()
)

null_customer_state = (
    customers
    .filter(col("customer_state").isNull())
    .count()
)

print("\nNULL customer_city:", null_customer_city)
print("NULL customer_state:", null_customer_state)

if null_customer_city == 0:
    print("PASS: No NULL customer_city.")
else:
    print("CHECK: NULL customer_city values found.")

if null_customer_state == 0:
    print("PASS: No NULL customer_state.")
else:
    print("CHECK: NULL customer_state values found.")


# --------------------------------------------------
# 13. Display Sample Data
# --------------------------------------------------

print(
    "\n========== SAMPLE GOLD CUSTOMER DATA ==========\n"
)

customers.show(
    10,
    truncate=False
)


# --------------------------------------------------
# 14. Customer Summary
# --------------------------------------------------

print("\n========== CUSTOMER SUMMARY ==========\n")

customers.select(
    "total_orders",
    "delivered_orders",
    "total_items",
    "total_spend",
    "average_order_value"
).describe().show()


# --------------------------------------------------
# 15. Final Validation Status
# --------------------------------------------------

print("\n========== VALIDATION COMPLETE ==========\n")

spark.stop()