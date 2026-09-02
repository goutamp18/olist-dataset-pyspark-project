from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower


spark = (
    SparkSession.builder
    .appName("OlistSilverCategoryTranslation")
    .master("local[*]")
    .getOrCreate()
)


BRONZE_PATH = "data/bronze/category_translation"

category_translation = spark.read.parquet(BRONZE_PATH)


# ============================================================
# CLEAN CATEGORY TRANSLATION DATA
# ============================================================

category_translation_clean = (
    category_translation
    .withColumn(
        "product_category_name",
        trim(lower(col("product_category_name")))
    )
    .withColumn(
        "product_category_name_english",
        trim(lower(col("product_category_name_english")))
    )
)


# ============================================================
# INSPECT CLEANED DATA
# ============================================================

print("\nCleaned Category Translation Data:")

category_translation_clean.show(
    20,
    truncate=False
)


# ============================================================
# NULL VALIDATION
# ============================================================

print(
    "NULL product_category_name:",
    category_translation_clean
    .filter(col("product_category_name").isNull())
    .count()
)

print(
    "NULL product_category_name_english:",
    category_translation_clean
    .filter(col("product_category_name_english").isNull())
    .count()
)


# ============================================================
# DUPLICATE VALIDATION
# ============================================================

duplicate_categories = (
    category_translation_clean
    .groupBy("product_category_name")
    .count()
    .filter(col("count") > 1)
)

print(
    "Duplicate product categories:",
    duplicate_categories.count()
)


# ============================================================
# WRITE SILVER DATA
# ============================================================

SILVER_PATH = "data/silver/category_translation"

category_translation_clean.write \
    .mode("overwrite") \
    .parquet(SILVER_PATH)


print("Silver category_translation written successfully.")


spark.stop()