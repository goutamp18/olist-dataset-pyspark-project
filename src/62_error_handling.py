from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# --------------------------------------------------
# 1. Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("ErrorHandling")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# 2. Paths
# --------------------------------------------------

source_path = "data/bronze/orders"

output_path = Path("data/gold/error_handling_demo")


# --------------------------------------------------
# 3. Pipeline Function
# --------------------------------------------------

def run_pipeline():

    print("\n========== PIPELINE STARTED ==========")

    try:

        # ------------------------------------------
        # Step 1: Read source
        # ------------------------------------------

        print("\n[1] Reading source data...")

        orders = spark.read.parquet(source_path)

        print("Source rows:", orders.count())


        # ------------------------------------------
        # Step 2: Validate required columns
        # ------------------------------------------

        print("\n[2] Validating required columns...")

        required_columns = [
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp"
        ]

        missing_columns = [
            column
            for column in required_columns
            if column not in orders.columns
        ]

        if missing_columns:

            raise ValueError(
                f"Missing required columns: {missing_columns}"
            )

        print("Schema validation: PASS")


        # ------------------------------------------
        # Step 3: Data validation
        # ------------------------------------------

        print("\n[3] Validating data...")

        invalid_orders = (
            orders
            .filter(
                col("order_id").isNull()
                |
                col("customer_id").isNull()
                |
                col("order_status").isNull()
            )
            .count()
        )

        print("Invalid records:", invalid_orders)

        if invalid_orders > 0:

            raise ValueError(
                f"Data quality validation failed: "
                f"{invalid_orders} invalid records"
            )

        print("Data quality validation: PASS")


        # ------------------------------------------
        # Step 4: Transformation
        # ------------------------------------------

        print("\n[4] Transforming data...")

        result = (
            orders
            .filter(col("order_status") == "delivered")
            .select(
                "order_id",
                "customer_id",
                "order_status",
                "order_purchase_timestamp"
            )
        )

        result_count = result.count()

        print("Transformed rows:", result_count)


        # ------------------------------------------
        # Step 5: Write output
        # ------------------------------------------

        print("\n[5] Writing output...")

        (
            result
            .write
            .mode("overwrite")
            .parquet(str(output_path))
        )

        print("Output written successfully.")


        # ------------------------------------------
        # Success
        # ------------------------------------------

        print("\n========== PIPELINE SUCCESS ==========")

        return True


    # --------------------------------------------------
    # Expected / validation errors
    # --------------------------------------------------

    except ValueError as error:

        print("\n========== PIPELINE VALIDATION FAILED ==========")

        print("Error:", error)

        return False


    # --------------------------------------------------
    # Unexpected errors
    # --------------------------------------------------

    except Exception as error:

        print("\n========== PIPELINE FAILED ==========")

        print("Error type:", type(error).__name__)

        print("Error:", error)

        return False


# --------------------------------------------------
# 4. Execute Pipeline
# --------------------------------------------------

pipeline_success = run_pipeline()


# --------------------------------------------------
# 5. Final Status
# --------------------------------------------------

print("\n========== FINAL STATUS ==========")

if pipeline_success:

    print("PIPELINE STATUS: SUCCESS")

else:

    print("PIPELINE STATUS: FAILED")


# --------------------------------------------------
# 6. Stop Spark
# --------------------------------------------------

spark.stop()