import time

from pyspark.sql import SparkSession


# --------------------------------------------------
# 1. Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("RetryMechanism")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# 2. Retry Configuration
# --------------------------------------------------

MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 2


# --------------------------------------------------
# 3. Simulated Pipeline Operation
# --------------------------------------------------

attempt_number = 0


def process_data():

    global attempt_number

    attempt_number += 1

    print(
        f"\nExecuting pipeline operation "
        f"(attempt {attempt_number})..."
    )

    # --------------------------------------------------
    # Simulate a temporary failure
    # --------------------------------------------------
    # First two attempts fail.
    # Third attempt succeeds.
    
    if attempt_number < 3:

        raise RuntimeError(
            "Simulated temporary failure"
        )

    # --------------------------------------------------
    # Real Spark operation
    # --------------------------------------------------

    orders = spark.read.parquet(
        "data/bronze/orders"
    )

    delivered_orders = orders.filter(
        orders.order_status == "delivered"
    )

    count = delivered_orders.count()

    print(
        f"Successfully processed {count} delivered orders."
    )


# --------------------------------------------------
# 4. Retry Wrapper
# --------------------------------------------------

def run_with_retry():

    for attempt in range(1, MAX_RETRIES + 1):

        try:

            print(
                f"\n========== ATTEMPT {attempt} =========="
            )

            process_data()

            print(
                "\nPIPELINE OPERATION: SUCCESS"
            )

            return True


        except Exception as error:

            print(
                f"\nAttempt {attempt} failed."
            )

            print(
                "Error type:",
                type(error).__name__
            )

            print(
                "Error:",
                error
            )


            # ------------------------------------------
            # Check whether retries remain
            # ------------------------------------------

            if attempt == MAX_RETRIES:

                print(
                    "\nMaximum retry attempts reached."
                )

                print(
                    "PIPELINE OPERATION: FAILED"
                )

                return False


            # ------------------------------------------
            # Wait before retry
            # ------------------------------------------

            print(
                f"Retrying in "
                f"{RETRY_DELAY_SECONDS} seconds..."
            )

            time.sleep(RETRY_DELAY_SECONDS)


# --------------------------------------------------
# 5. Run Pipeline
# --------------------------------------------------

print("\n========== RETRY PIPELINE STARTED ==========")

success = run_with_retry()


# --------------------------------------------------
# 6. Final Status
# --------------------------------------------------

print("\n========== FINAL STATUS ==========")

if success:

    print("PIPELINE STATUS: SUCCESS")

else:

    print("PIPELINE STATUS: FAILED")


# --------------------------------------------------
# 7. Stop Spark
# --------------------------------------------------

spark.stop()