from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, round, when


spark = (
    SparkSession.builder
    .appName("OlistSilverOrderPayments")
    .master("local[*]")
    .getOrCreate()
)


BRONZE_PATH = "data/bronze/order_payments"

payments = spark.read.parquet(BRONZE_PATH)


# ============================================================
# CLEAN PAYMENT DATA
# ============================================================

payments_clean = (
    payments
    .withColumn(
        "order_id",
        trim(col("order_id"))
    )
    .withColumn(
        "payment_type",
        lower(trim(col("payment_type")))
    )
    .withColumn(
        "payment_installments",
        when(
            col("payment_installments") <= 0,
            None
        ).otherwise(
            col("payment_installments")
        )
    )
    .withColumn(
        "payment_value",
        round(col("payment_value"), 2)
    )
)


# ============================================================
# INSPECT CLEANED DATA
# ============================================================

print("\nCleaned Payment Data:")

payments_clean.show(
    10,
    truncate=False
)


# ============================================================
# NULL VALIDATION
# ============================================================

print(
    "NULL order_id:",
    payments_clean
    .filter(col("order_id").isNull())
    .count()
)

print(
    "NULL payment_type:",
    payments_clean
    .filter(col("payment_type").isNull())
    .count()
)

print(
    "NULL payment_value:",
    payments_clean
    .filter(col("payment_value").isNull())
    .count()
)

print(
    "NULL payment_installments:",
    payments_clean
    .filter(col("payment_installments").isNull())
    .count()
)


# ============================================================
# NUMERIC VALIDATION
# ============================================================

invalid_payment_value = payments_clean.filter(
    col("payment_value") < 0
)

print(
    "Invalid payment value:",
    invalid_payment_value.count()
)


# ============================================================
# WRITE SILVER DATA
# ============================================================

SILVER_PATH = "data/silver/order_payments"

payments_clean.write \
    .mode("overwrite") \
    .parquet(SILVER_PATH)


print("Silver order_payments written successfully.")


spark.stop()