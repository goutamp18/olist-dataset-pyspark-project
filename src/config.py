from pathlib import Path
import os


# --------------------------------------------------
# Environment
# --------------------------------------------------

ENV = os.getenv("APP_ENV", "dev")


# --------------------------------------------------
# Project paths
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATA_DIR = PROJECT_ROOT / "data"

RAW_DIR = DATA_DIR / "raw"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

LOG_DIR = PROJECT_ROOT / "logs"


# --------------------------------------------------
# Spark configuration
# --------------------------------------------------

SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")


# --------------------------------------------------
# Print configuration
# --------------------------------------------------

if __name__ == "__main__":
    print("Environment:", ENV)
    print("Project Root:", PROJECT_ROOT)
    print("Raw Directory:", RAW_DIR)
    print("Bronze Directory:", BRONZE_DIR)
    print("Silver Directory:", SILVER_DIR)
    print("Gold Directory:", GOLD_DIR)
    print("Log Directory:", LOG_DIR)
    print("Spark Master:", SPARK_MASTER)
    