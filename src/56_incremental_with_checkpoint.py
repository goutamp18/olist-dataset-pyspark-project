from pathlib import Path
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max


spark = (
    SparkSession.builder
    .appName("IncrementalWithCheckpoint")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# 1. Configuration
# --------------------------------------------------

orders_path = "data/bronze/orders"
checkpoint_path = Path("data/checkpoints/orders_checkpoint.txt")

# Create checkpoint directory if needed
checkpoint_path.parent.mkdir(parents=True, exist_ok=True)


# --------------------------------------------------
# 2. Read source data
# --------------------------------------------------

orders = spark.read.parquet(orders_path)


# --------------------------------------------------
# 3. Create initial checkpoint if it doesn't exist
# --------------------------------------------------

if not checkpoint_path.exists():

    initial_checkpoint = (
        orders
        .select(
            spark_max("order_purchase_timestamp")
        )
        .collect()[0][0]
    )

    # For demonstration, start from the beginning
    # of the dataset instead of the maximum timestamp.
    initial_checkpoint = "2018-08-01 00:00:00"

    checkpoint_path.write_text(initial_checkpoint)

    print("Created initial checkpoint:")
    print(initial_checkpoint)


# --------------------------------------------------
# 4. Read checkpoint
# --------------------------------------------------

last_processed = checkpoint_path.read_text().strip()

print("\nLast processed timestamp:")
print(last_processed)


# --------------------------------------------------
# 5. Define incremental window
# --------------------------------------------------

incremental_orders = orders.filter(
    col("order_purchase_timestamp") > last_processed
)


print("\n========== INCREMENTAL WINDOW ==========")

print(
    "Orders to process:",
    incremental_orders.count()
)


# --------------------------------------------------
# 6. Determine new checkpoint
# --------------------------------------------------

new_checkpoint = (
    incremental_orders
    .select(
        spark_max("order_purchase_timestamp")
        .alias("max_timestamp")
    )
    .collect()[0]["max_timestamp"]
)


print("\nNew checkpoint candidate:")
print(new_checkpoint)


# --------------------------------------------------
# 7. Simulate successful processing
# --------------------------------------------------

processing_successful = True


if processing_successful:

    if new_checkpoint is not None:

        checkpoint_path.write_text(
            new_checkpoint.strftime("%Y-%m-%d %H:%M:%S")
        )

        print("\nProcessing successful.")
        print("Checkpoint updated.")

    else:

        print("\nNo new data.")
        print("Checkpoint unchanged.")


# --------------------------------------------------
# 8. Read checkpoint again
# --------------------------------------------------

print("\n========== FINAL CHECKPOINT ==========")

print(
    checkpoint_path.read_text().strip()
)


spark.stop()