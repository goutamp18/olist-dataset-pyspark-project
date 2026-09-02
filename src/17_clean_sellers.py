from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper


spark = (
    SparkSession.builder
    .appName("OlistSilverSellers")
    .master("local[*]")
    .getOrCreate()
)


BRONZE_PATH = "data/bronze/sellers"

sellers = spark.read.parquet(BRONZE_PATH)


# ============================================================
# CLEAN SELLER DATA
# ============================================================

sellers_clean = (
    sellers
    .withColumn(
        "seller_id",
        trim(col("seller_id"))
    )
    .withColumn(
        "seller_city",
        trim(col("seller_city"))
    )
    .withColumn(
        "seller_state",
        upper(trim(col("seller_state")))
    )
)


# ============================================================
# INSPECT CLEANED DATA
# ============================================================

print("\nCleaned Seller Data:")

sellers_clean.show(
    10,
    truncate=False
)


# ============================================================
# NULL VALIDATION
# ============================================================

print(
    "NULL seller_id:",
    sellers_clean
    .filter(col("seller_id").isNull())
    .count()
)

print(
    "NULL seller_city:",
    sellers_clean
    .filter(col("seller_city").isNull())
    .count()
)

print(
    "NULL seller_state:",
    sellers_clean
    .filter(col("seller_state").isNull())
    .count()
)


# ============================================================
# ZIP CODE VALIDATION
# ============================================================

invalid_zip_codes = sellers_clean.filter(
    col("seller_zip_code_prefix") < 0
)

print(
    "Invalid seller zip codes:",
    invalid_zip_codes.count()
)


# ============================================================
# WRITE SILVER DATA
# ============================================================

SILVER_PATH = "data/silver/sellers"

sellers_clean.write \
    .mode("overwrite") \
    .parquet(SILVER_PATH)


print("Silver sellers written successfully.")


spark.stop()