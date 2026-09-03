from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType
)


# --------------------------------------------------
# 1. Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("SchemaValidation")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# 2. Paths
# --------------------------------------------------

orders_path = "data/bronze/orders"


# --------------------------------------------------
# 3. Expected Schema
# --------------------------------------------------

expected_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("order_status", StringType(), False),
    StructField(
        "order_purchase_timestamp",
        TimestampType(),
        False
    ),
    StructField(
        "order_approved_at",
        TimestampType(),
        True
    ),
    StructField(
        "order_delivered_carrier_date",
        TimestampType(),
        True
    ),
    StructField(
        "order_delivered_customer_date",
        TimestampType(),
        True
    ),
    StructField(
        "order_estimated_delivery_date",
        TimestampType(),
        False
    )
])


# --------------------------------------------------
# 4. Read Incoming Data
# --------------------------------------------------

orders = spark.read.parquet(orders_path)


# --------------------------------------------------
# 5. Display Schemas
# --------------------------------------------------

print("\n========== EXPECTED SCHEMA ==========")

print(expected_schema.simpleString())


print("\n========== ACTUAL SCHEMA ==========")

orders.printSchema()


# --------------------------------------------------
# 6. Extract Schema Information
# --------------------------------------------------

expected_fields = {
    field.name: field.dataType.simpleString()
    for field in expected_schema.fields
}

actual_fields = {
    field.name: field.dataType.simpleString()
    for field in orders.schema.fields
}


# --------------------------------------------------
# 7. Check Missing Columns
# --------------------------------------------------

missing_columns = [
    column
    for column in expected_fields
    if column not in actual_fields
]


# --------------------------------------------------
# 8. Check Unexpected Columns
# --------------------------------------------------

unexpected_columns = [
    column
    for column in actual_fields
    if column not in expected_fields
]


# --------------------------------------------------
# 9. Check Data Types
# --------------------------------------------------

type_mismatches = []

for column in expected_fields:

    if column in actual_fields:

        expected_type = expected_fields[column]
        actual_type = actual_fields[column]

        if expected_type != actual_type:

            type_mismatches.append({
                "column": column,
                "expected": expected_type,
                "actual": actual_type
            })


# --------------------------------------------------
# 10. Print Validation Results
# --------------------------------------------------

print("\n========== SCHEMA VALIDATION ==========")

print("Missing columns:")
print(missing_columns)

print("\nUnexpected columns:")
print(unexpected_columns)

print("\nData type mismatches:")

if type_mismatches:
    for mismatch in type_mismatches:
        print(mismatch)
else:
    print([])


# --------------------------------------------------
# 11. Final Validation
# --------------------------------------------------

schema_valid = (
    len(missing_columns) == 0
    and len(unexpected_columns) == 0
    and len(type_mismatches) == 0
)


print("\n========== RESULT ==========")

if schema_valid:
    print("SCHEMA VALIDATION: PASS")
else:
    print("SCHEMA VALIDATION: FAIL")


# --------------------------------------------------
# 12. Stop Spark
# --------------------------------------------------

spark.stop()