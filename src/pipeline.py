import logging
import os
import time

from src.config import (
    ENV,
    SPARK_MASTER,
    BRONZE_DIR,
    SILVER_DIR,
    GOLD_DIR
)

from src.bronze import run_bronze
from src.silver import run_silver
from src.gold import run_gold

from src.pipeline_metrics import LayerMetrics


# ============================================================
# Logging Configuration
# ============================================================

LOG_DIR = "logs"

os.makedirs(
    LOG_DIR,
    exist_ok=True
)

LOG_FILE = os.path.join(
    LOG_DIR,
    "pipeline.log"
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE)
    ]
)

logger = logging.getLogger(__name__)
BRONZE_DATASETS = [
    "customers",
    "geolocation",
    "order_items",
    "order_payments",
    "order_reviews",
    "orders",
    "products",
    "sellers",
    "category_translation"
]

SILVER_DATASETS = [
    "orders",
    "customers",
    "order_items",
    "order_payments",
    "products",
    "sellers",
    "order_reviews",
    "geolocation",
    "category_translation"
]

GOLD_DATASETS = [
    "orders",
    "customers",
    "products",
    "sellers",
    "category",
    "reviews",
    "payments",
    "geolocation"
]

# ============================================================
# Spark Session
# ============================================================

def create_spark_session():

    from pyspark.sql import SparkSession

    return (
        SparkSession.builder
        .appName(
            f"OlistProductionPipeline-{ENV}"
        )
        .master(SPARK_MASTER)
        .getOrCreate()
    )


# ============================================================
# Count Rows From Layer
# ============================================================

def count_layer_rows(spark, layer_path, expected_datasets):
    if not os.path.exists(layer_path):
        logger.warning(f"Layer path does not exist: {layer_path}")
        return 0, 0

    total_rows = 0
    dataset_count = 0

    for dataset_name in expected_datasets:
        dataset_path = os.path.join(layer_path, dataset_name)

        if not os.path.isdir(dataset_path):
            logger.warning(
                f"Expected dataset does not exist: {dataset_path}"
            )
            continue

        try:
            df = spark.read.parquet(dataset_path)
            row_count = df.count()

            total_rows += row_count
            dataset_count += 1

            logger.info(
                f"Metrics | {dataset_name} | rows={row_count}"
            )

        except Exception as e:
            logger.warning(
                f"Could not count dataset {dataset_name}: {str(e)}"
            )

    return total_rows, dataset_count
# ============================================================
# Log Layer Metrics
# ============================================================

def log_layer_metrics(metrics):

    logger.info("=" * 60)
    logger.info(
        f"{metrics.layer_name.upper()} METRICS"
    )
    logger.info("=" * 60)

    logger.info(
        f"Status            : {metrics.status}"
    )

    logger.info(
        f"Datasets processed: "
        f"{metrics.datasets_processed}"
    )

    logger.info(
        f"Input rows        : "
        f"{metrics.input_rows}"
    )

    logger.info(
        f"Output rows       : "
        f"{metrics.output_rows}"
    )

    logger.info(
        f"Duration          : "
        f"{metrics.duration} seconds"
    )


# ============================================================
# Main Pipeline
# ============================================================

def run_pipeline():

    pipeline_start = time.time()

    spark = None

    bronze_metrics = None
    silver_metrics = None
    gold_metrics = None

    try:

        # ====================================================
        # PIPELINE START
        # ====================================================

        logger.info("=" * 60)
        logger.info(
            "OLIST PRODUCTION PIPELINE"
        )
        logger.info("=" * 60)

        logger.info(
            f"Environment: {ENV}"
        )

        # ====================================================
        # CREATE SPARK SESSION
        # ====================================================

        spark = create_spark_session()

        logger.info(
            f"Spark Version: {spark.version}"
        )

        logger.info(
            f"Spark Master: {SPARK_MASTER}"
        )

        # ====================================================
        # BRONZE
        # ====================================================

        logger.info("=" * 60)
        logger.info(
            "STARTING BRONZE LAYER"
        )
        logger.info("=" * 60)

        bronze_metrics = LayerMetrics(
            "Bronze"
        )

        bronze_metrics.start()

        try:

            # Run Bronze processing
            bronze_rows, bronze_datasets = count_layer_rows(
    spark,
    BRONZE_DIR,
    BRONZE_DATASETS
)
            

            bronze_metrics.set_output_rows(
                bronze_rows
            )

            bronze_metrics.set_datasets_processed(
                bronze_datasets
            )

            bronze_metrics.success()

        except Exception:

            bronze_metrics.failed()

            logger.exception(
                "Bronze layer failed"
            )

            raise

        log_layer_metrics(
            bronze_metrics
        )

        # ====================================================
        # SILVER
        # ====================================================

        logger.info("=" * 60)
        logger.info(
            "STARTING SILVER LAYER"
        )
        logger.info("=" * 60)

        silver_metrics = LayerMetrics(
            "Silver"
        )

        silver_metrics.start()

        try:

            # Bronze becomes Silver input
            silver_metrics.set_input_rows(
                bronze_metrics.output_rows
            )

            # Run Silver processing
            run_silver(spark)

            # Count generated Silver data
            silver_rows, silver_datasets = count_layer_rows(
    spark,
    SILVER_DIR,
    SILVER_DATASETS
)

            silver_metrics.set_output_rows(
                silver_rows
            )

            silver_metrics.set_datasets_processed(
                silver_datasets
            )

            silver_metrics.success()

        except Exception:

            silver_metrics.failed()

            logger.exception(
                "Silver layer failed"
            )

            raise

        log_layer_metrics(
            silver_metrics
        )

        # ====================================================
        # GOLD
        # ====================================================

        logger.info("=" * 60)
        logger.info(
            "STARTING GOLD LAYER"
        )
        logger.info("=" * 60)

        gold_metrics = LayerMetrics(
            "Gold"
        )

        gold_metrics.start()

        try:

            # Silver becomes Gold input
            gold_metrics.set_input_rows(
                silver_metrics.output_rows
            )

            # Run Gold processing
            run_gold(spark)

            # Count generated Gold data
            gold_rows, gold_datasets = count_layer_rows(
    spark,
    GOLD_DIR,
    GOLD_DATASETS
)

            gold_metrics.set_output_rows(
                gold_rows
            )

            gold_metrics.set_datasets_processed(
                gold_datasets
            )

            gold_metrics.success()

        except Exception:

            gold_metrics.failed()

            logger.exception(
                "Gold layer failed"
            )

            raise

        log_layer_metrics(
            gold_metrics
        )

        # ====================================================
        # PIPELINE SUMMARY
        # ====================================================

        total_time = round(
            time.time() - pipeline_start,
            2
        )

        logger.info("=" * 60)
        logger.info(
            "PIPELINE EXECUTION SUMMARY"
        )
        logger.info("=" * 60)

        logger.info(
            f"Bronze runtime : "
            f"{bronze_metrics.duration} seconds"
        )

        logger.info(
            f"Silver runtime : "
            f"{silver_metrics.duration} seconds"
        )

        logger.info(
            f"Gold runtime   : "
            f"{gold_metrics.duration} seconds"
        )

        logger.info(
            f"Total runtime  : "
            f"{total_time} seconds"
        )

        # ====================================================
        # DATA QUALITY SUMMARY
        # ====================================================

        logger.info("=" * 60)
        logger.info(
            "DATA QUALITY SUMMARY"
        )
        logger.info("=" * 60)

        logger.info(
            f"Bronze output rows : "
            f"{bronze_metrics.output_rows}"
        )

        logger.info(
            f"Silver output rows : "
            f"{silver_metrics.output_rows}"
        )

        logger.info(
            f"Gold output rows   : "
            f"{gold_metrics.output_rows}"
        )

        # ----------------------------------------------------
        # Row Count Comparison
        # ----------------------------------------------------

        if (
            silver_metrics.output_rows
            <= bronze_metrics.output_rows
        ):

            logger.info(
                "Bronze -> Silver row count: "
                "VALID"
            )

        else:

            logger.warning(
                "Bronze -> Silver row count: "
                "INCREASED"
            )

        if (
            gold_metrics.output_rows
            <= silver_metrics.output_rows
        ):

            logger.info(
                "Silver -> Gold row count: "
                "VALID"
            )

        else:

            logger.warning(
                "Silver -> Gold row count: "
                "INCREASED"
            )

        # ====================================================
        # FINAL STATUS
        # ====================================================

        logger.info("=" * 60)
        logger.info(
            "PIPELINE STATUS: SUCCESS"
        )
        logger.info("=" * 60)

    # ========================================================
    # PIPELINE FAILURE
    # ========================================================

    except Exception:

        total_time = round(
            time.time() - pipeline_start,
            2
        )

        logger.error("=" * 60)
        logger.error(
            "PIPELINE STATUS: FAILED"
        )
        logger.error("=" * 60)

        logger.error(
            f"Runtime before failure: "
            f"{total_time} seconds"
        )

        logger.exception(
            "Pipeline execution failed"
        )

        logger.error("=" * 60)

        # Re-raise the exception so that
        # schedulers/orchestrators can detect
        # the failed pipeline.

        raise

    # ========================================================
    # ALWAYS STOP SPARK
    # ========================================================

    finally:

        if spark is not None:

            logger.info(
                "Stopping Spark session"
            )

            spark.stop()

            logger.info(
                "Spark session stopped"
            )


# ============================================================
# Entry Point
# ============================================================

if __name__ == "__main__":

    run_pipeline()

