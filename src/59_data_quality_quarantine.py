from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    when,
    concat_ws
)


# --------------------------------------------------
# 1. Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("DataQualityQuarantine")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# 2. Paths
# --------------------------------------------------

orders_path = "data/bronze/orders"

valid_output_path = "data/silver/quality_checked_orders"

quarantine_output_path = "data/silver/quarantine/orders"


# --------------------------------------------------
# 3. Read Bronze Orders
# --------------------------------------------------

orders = spark.read.parquet(orders_path)


print("\n========== SOURCE DATA ==========")

print("Total source rows:", orders.count())


# --------------------------------------------------
# 4. Simulate Bad Records
# --------------------------------------------------
# We intentionally create invalid records so that
# we can demonstrate the quarantine process.
#
# Real production pipelines would receive these
# bad records from the source system itself.

test_data = (
    orders
    .select(
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_customer_date"
    )
    .limit(10)
)


bad_records = (
    test_data
    .withColumn(
        "order_id",
        when(
            col("order_id") == test_data.first()["order_id"],
            lit(None)
        ).otherwise(col("order_id"))
    )
    .withColumn(
        "order_status",
        when(
            col("order_status") == test_data.first()["order_status"],
            lit("INVALID_STATUS")
        ).otherwise(col("order_status"))
    )
)


# --------------------------------------------------
# 5. Combine Original + Bad Records
# --------------------------------------------------

test_data_with_bad_records = orders.select(
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_customer_date"
).unionByName(
    bad_records
)


print("\n========== TEST DATA ==========")

print(
    "Rows after adding simulated bad records:",
    test_data_with_bad_records.count()
)


# --------------------------------------------------
# 6. Define Data Quality Rules
# --------------------------------------------------

valid_statuses = [
    "approved",
    "canceled",
    "created",
    "delivered",
    "invoiced",
    "processing",
    "shipped",
    "unavailable"
]


# --------------------------------------------------
# 7. Create Validation Flags
# --------------------------------------------------

validated = (
    test_data_with_bad_records

    # Rule 1: order_id must not be NULL
    .withColumn(
        "invalid_order_id",
        col("order_id").isNull()
    )

    # Rule 2: customer_id must not be NULL
    .withColumn(
        "invalid_customer_id",
        col("customer_id").isNull()
    )

    # Rule 3: order_status must be valid
    .withColumn(
        "invalid_status",
        ~col("order_status").isin(valid_statuses)
    )

    # Rule 4: purchase timestamp must exist
    .withColumn(
        "invalid_purchase_timestamp",
        col("order_purchase_timestamp").isNull()
    )

    # Rule 5: delivered date cannot be before purchase date
    .withColumn(
        "invalid_delivery_date",
        (
            col("order_delivered_customer_date").isNotNull()
            &
            (
                col("order_delivered_customer_date")
                < col("order_purchase_timestamp")
            )
        )
    )
)


# --------------------------------------------------
# 8. Generate Rejection Reason
# --------------------------------------------------

validated = validated.withColumn(
    "rejection_reason",
    concat_ws(
        ", ",
        when(
            col("invalid_order_id"),
            lit("NULL_ORDER_ID")
        ),
        when(
            col("invalid_customer_id"),
            lit("NULL_CUSTOMER_ID")
        ),
        when(
            col("invalid_status"),
            lit("INVALID_ORDER_STATUS")
        ),
        when(
            col("invalid_purchase_timestamp"),
            lit("NULL_PURCHASE_TIMESTAMP")
        ),
        when(
            col("invalid_delivery_date"),
            lit("DELIVERY_BEFORE_PURCHASE")
        )
    )
)


# --------------------------------------------------
# 9. Split Valid and Invalid Records
# --------------------------------------------------

invalid_condition = (
    col("invalid_order_id")
    | col("invalid_customer_id")
    | col("invalid_status")
    | col("invalid_purchase_timestamp")
    | col("invalid_delivery_date")
)


valid_records = (
    validated
    .filter(~invalid_condition)
    .drop(
        "invalid_order_id",
        "invalid_customer_id",
        "invalid_status",
        "invalid_purchase_timestamp",
        "invalid_delivery_date",
        "rejection_reason"
    )
)


quarantine_records = (
    validated
    .filter(invalid_condition)
    .withColumn(
        "quarantine_timestamp",
        lit(None).cast("timestamp")
    )
)


# --------------------------------------------------
# 10. Counts
# --------------------------------------------------

valid_count = valid_records.count()

quarantine_count = quarantine_records.count()


print("\n========== DATA QUALITY RESULT ==========")

print("Valid records:", valid_count)

print("Quarantined records:", quarantine_count)


# --------------------------------------------------
# 11. Display Quarantine Records
# --------------------------------------------------

print("\n========== QUARANTINE RECORDS ==========")

quarantine_records.select(
    "order_id",
    "customer_id",
    "order_status",
    "rejection_reason"
).show(
    truncate=False
)


# --------------------------------------------------
# 12. Write Valid Records
# --------------------------------------------------

(
    valid_records
    .write
    .mode("overwrite")
    .parquet(valid_output_path)
)


# --------------------------------------------------
# 13. Write Quarantine Records
# --------------------------------------------------

(
    quarantine_records
    .write
    .mode("overwrite")
    .parquet(quarantine_output_path)
)


# --------------------------------------------------
# 14. Final Validation
# --------------------------------------------------

print("\n========== FINAL VALIDATION ==========")

print(
    "Valid output rows:",
    spark.read.parquet(valid_output_path).count()
)

print(
    "Quarantine output rows:",
    spark.read.parquet(quarantine_output_path).count()
)


print("\n========== RESULT ==========")

if valid_count + quarantine_count == test_data_with_bad_records.count():
    print("DATA QUALITY PIPELINE: PASS")
else:
    print("DATA QUALITY PIPELINE: FAIL")


# --------------------------------------------------
# 15. Stop Spark
# --------------------------------------------------

spark.stop()