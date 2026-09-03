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
    .appName("SchemaEvolution")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# 2. Expected Production Schema
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
# 3. Read Current Data
# --------------------------------------------------

orders = spark.read.parquet(
    "data/bronze/orders"
)

actual_schema = orders.schema


# --------------------------------------------------
# 4. Helper Function
# --------------------------------------------------

def schema_to_dict(schema):

    return {
        field.name: field.dataType.simpleString()
        for field in schema.fields
    }


expected_fields = schema_to_dict(expected_schema)
actual_fields = schema_to_dict(actual_schema)


# --------------------------------------------------
# 5. Display Current Schema
# --------------------------------------------------

print("\n========== CURRENT SCHEMA ==========")

print(actual_schema.simpleString())


# --------------------------------------------------
# 6. Scenario 1
# --------------------------------------------------
# No schema change

scenario_1 = actual_fields.copy()


# --------------------------------------------------
# 7. Scenario 2
# --------------------------------------------------
# New column added

scenario_2 = actual_fields.copy()

scenario_2["customer_email"] = "string"


# --------------------------------------------------
# 8. Scenario 3
# --------------------------------------------------
# Existing column changes type
#
# order_status:
# string → integer
#
# This is a BREAKING CHANGE.

scenario_3 = actual_fields.copy()

scenario_3["order_status"] = "int"


# --------------------------------------------------
# 9. Schema Comparison Function
# --------------------------------------------------

def compare_schema(expected, incoming):

    missing_columns = [
        column
        for column in expected
        if column not in incoming
    ]

    new_columns = [
        column
        for column in incoming
        if column not in expected
    ]

    type_changes = []

    for column in expected:

        if column in incoming:

            if expected[column] != incoming[column]:

                type_changes.append({
                    "column": column,
                    "expected": expected[column],
                    "incoming": incoming[column]
                })

    return (
        missing_columns,
        new_columns,
        type_changes
    )


# --------------------------------------------------
# 10. Schema Evaluation
# --------------------------------------------------

def evaluate_schema(
    scenario_name,
    expected,
    incoming,
    allow_new_columns=False
):

    missing_columns, new_columns, type_changes = (
        compare_schema(expected, incoming)
    )

    print(f"\n========== {scenario_name} ==========")

    print("Missing columns:")
    print(missing_columns)

    print("\nNew columns:")
    print(new_columns)

    print("\nType changes:")
    print(type_changes)

    # ----------------------------------------------
    # Breaking changes
    # ----------------------------------------------

    if missing_columns:

        print("\nRESULT: REJECT")

        print(
            "Reason: Required columns are missing."
        )

        return False

    if type_changes:

        print("\nRESULT: REJECT")

        print(
            "Reason: Existing column data types changed."
        )

        return False

    # ----------------------------------------------
    # New columns
    # ----------------------------------------------

    if new_columns and not allow_new_columns:

        print("\nRESULT: REJECT")

        print(
            "Reason: New columns are not allowed "
            "under strict schema policy."
        )

        return False

    # ----------------------------------------------
    # Valid
    # ----------------------------------------------

    print("\nRESULT: ACCEPT")

    return True


# --------------------------------------------------
# 11. Scenario 1
# --------------------------------------------------

evaluate_schema(
    "SCENARIO 1 - NO CHANGE",
    expected_fields,
    scenario_1,
    allow_new_columns=False
)


# --------------------------------------------------
# 12. Scenario 2
# --------------------------------------------------

evaluate_schema(
    "SCENARIO 2 - NEW COLUMN",
    expected_fields,
    scenario_2,
    allow_new_columns=False
)


# --------------------------------------------------
# 13. Scenario 2 Again
# --------------------------------------------------
# Demonstrate schema evolution policy where
# adding a new column is allowed.

evaluate_schema(
    "SCENARIO 2 - NEW COLUMN ALLOWED",
    expected_fields,
    scenario_2,
    allow_new_columns=True
)


# --------------------------------------------------
# 14. Scenario 3
# --------------------------------------------------

evaluate_schema(
    "SCENARIO 3 - TYPE CHANGE",
    expected_fields,
    scenario_3,
    allow_new_columns=True
)


# --------------------------------------------------
# 15. Production Rules
# --------------------------------------------------

print("\n========== PRODUCTION RULES ==========")

print("1. Missing required column       → REJECT")
print("2. Existing type changed        → REJECT")
print("3. New column + strict policy   → REJECT")
print("4. New column + evolution       → ACCEPT")
print("5. Compatible schema            → ACCEPT")


# --------------------------------------------------
# 16. Stop Spark
# --------------------------------------------------

spark.stop()