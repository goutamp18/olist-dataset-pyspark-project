from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# --------------------------------------------------
# 1. Create Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("IdempotentProcessing")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# 2. Configuration
# --------------------------------------------------

orders_path = "data/bronze/orders"
output_path = Path("data/gold/idempotent_orders")


# --------------------------------------------------
# 3. Read source data
# --------------------------------------------------

orders = spark.read.parquet(orders_path)


# --------------------------------------------------
# 4. Create the batch
# --------------------------------------------------

batch = (
    orders
    .select(
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp"
    )
    .filter(
        col("order_purchase_timestamp") >= "2018-08-01"
    )
    .filter(
        col("order_purchase_timestamp") < "2018-08-08"
    )
)


print("\n========== SOURCE BATCH ==========")

batch_count = batch.count()

print("Batch rows:", batch_count)


# --------------------------------------------------
# 5. FIRST RUN
# --------------------------------------------------

print("\n========== FIRST RUN ==========")

(
    batch
    .write
    .mode("overwrite")
    .parquet(str(output_path))
)


# IMPORTANT:
# Count the first run BEFORE overwriting
# the same output path again.

first_run = spark.read.parquet(str(output_path))

first_count = first_run.count()

print("Rows after first run:", first_count)


# --------------------------------------------------
# 6. SECOND RUN
# --------------------------------------------------

print("\n========== SECOND RUN ==========")

# Simulate the exact same batch arriving again.

second_batch = batch


(
    second_batch
    .write
    .mode("overwrite")
    .parquet(str(output_path))
)


second_run = spark.read.parquet(str(output_path))

second_count = second_run.count()

print("Rows after second run:", second_count)


# --------------------------------------------------
# 7. Check duplicate order IDs
# --------------------------------------------------

duplicate_count = (
    second_run
    .groupBy("order_id")
    .count()
    .filter(col("count") > 1)
    .count()
)


print("\n========== VALIDATION ==========")

print("Duplicate order IDs:", duplicate_count)


# --------------------------------------------------
# 8. Validate idempotency
# --------------------------------------------------

print("\n========== RESULT ==========")

if (
    first_count == second_count
    and duplicate_count == 0
):
    print("IDEMPOTENCY CHECK: PASS")
else:
    print("IDEMPOTENCY CHECK: FAIL")


# --------------------------------------------------
# 9. Stop Spark
# --------------------------------------------------

spark.stop()