from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = (
    SparkSession.builder
    .appName("ValidateSilverOrderPayments")
    .master("local[*]")
    .getOrCreate()
)


SILVER_PATH = "data/silver/order_payments"

payments = spark.read.parquet(SILVER_PATH)


# ============================================================
# ROW COUNT
# ============================================================

print(
    "Silver Order Payments Count:",
    payments.count()
)


# ============================================================
# SCHEMA
# ============================================================

print("\nSilver Order Payments Schema:")

payments.printSchema()


# ============================================================
# SAMPLE DATA
# ============================================================

print("\nSilver Order Payments Sample:")

payments.show(
    10,
    truncate=False
)


# ============================================================
# NULL VALIDATION
# ============================================================

print(
    "NULL order_id:",
    payments
    .filter(col("order_id").isNull())
    .count()
)

print(
    "NULL payment_type:",
    payments
    .filter(col("payment_type").isNull())
    .count()
)

print(
    "NULL payment_value:",
    payments
    .filter(col("payment_value").isNull())
    .count()
)

print(
    "NULL payment_installments:",
    payments
    .filter(col("payment_installments").isNull())
    .count()
)


# ============================================================
# PAYMENT VALUE VALIDATION
# ============================================================

invalid_payment_value = payments.filter(
    col("payment_value") < 0
)

print(
    "Invalid payment value:",
    invalid_payment_value.count()
)


# ============================================================
# PAYMENT TYPE DISTRIBUTION
# ============================================================

print("\nPayment Type Distribution:")

payments.groupBy(
    "payment_type"
).count().orderBy(
    col("count").desc()
).show()


# ============================================================
# FINISH
# ============================================================

spark.stop()
