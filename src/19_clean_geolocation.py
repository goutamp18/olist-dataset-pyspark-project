
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper


spark = (
    SparkSession.builder
    .appName("OlistSilverGeolocation")
    .master("local[*]")
    .getOrCreate()
)


BRONZE_PATH = "data/bronze/geolocation"

geolocation = spark.read.parquet(BRONZE_PATH)


# ============================================================
# CLEAN GEOLOCATION DATA
# ============================================================

geolocation_clean = (
    geolocation
    .withColumn(
        "geolocation_city",
        trim(col("geolocation_city"))
    )
    .withColumn(
        "geolocation_state",
        upper(trim(col("geolocation_state")))
    )
)


# ============================================================
# INSPECT CLEANED DATA
# ============================================================

print("\nCleaned Geolocation Data:")

geolocation_clean.show(
    10,
    truncate=False
)


# ============================================================
# NULL VALIDATION
# ============================================================

print(
    "NULL geolocation_zip_code_prefix:",
    geolocation_clean
    .filter(col("geolocation_zip_code_prefix").isNull())
    .count()
)

print(
    "NULL geolocation_lat:",
    geolocation_clean
    .filter(col("geolocation_lat").isNull())
    .count()
)

print(
    "NULL geolocation_lng:",
    geolocation_clean
    .filter(col("geolocation_lng").isNull())
    .count()
)

print(
    "NULL geolocation_city:",
    geolocation_clean
    .filter(col("geolocation_city").isNull())
    .count()
)

print(
    "NULL geolocation_state:",
    geolocation_clean
    .filter(col("geolocation_state").isNull())
    .count()
)


# ============================================================
# COORDINATE VALIDATION
# ============================================================

invalid_latitude = geolocation_clean.filter(
    (col("geolocation_lat") < -90) |
    (col("geolocation_lat") > 90)
)

print(
    "Invalid latitude:",
    invalid_latitude.count()
)


invalid_longitude = geolocation_clean.filter(
    (col("geolocation_lng") < -180) |
    (col("geolocation_lng") > 180)
)

print(
    "Invalid longitude:",
    invalid_longitude.count()
)


# ============================================================
# WRITE SILVER DATA
# ============================================================

SILVER_PATH = "data/silver/geolocation"

geolocation_clean.write \
    .mode("overwrite") \
    .parquet(SILVER_PATH)


print("Silver geolocation written successfully.")


spark.stop()