from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, when


spark = (
    SparkSession.builder
    .appName("OlistSilverProducts")
    .master("local[*]")
    .getOrCreate()
)


BRONZE_PATH = "data/bronze/products"

products = spark.read.parquet(BRONZE_PATH)


# ============================================================
# CLEAN PRODUCT DATA
# ============================================================

products_clean = (
    products
    .withColumn(
        "product_id",
        trim(col("product_id"))
    )
    .withColumn(
        "product_category_name",
        trim(lower(col("product_category_name")))
    )
    .withColumn(
        "product_name_lenght",
        when(
            col("product_name_lenght") < 0,
            None
        ).otherwise(col("product_name_lenght"))
    )
    .withColumn(
        "product_description_lenght",
        when(
            col("product_description_lenght") < 0,
            None
        ).otherwise(col("product_description_lenght"))
    )
    .withColumn(
        "product_photos_qty",
        when(
            col("product_photos_qty") < 0,
            None
        ).otherwise(col("product_photos_qty"))
    )
    .withColumn(
        "product_weight_g",
        when(
            col("product_weight_g") <= 0,
            None
        ).otherwise(col("product_weight_g"))
    )
    .withColumn(
        "product_length_cm",
        when(
            col("product_length_cm") <= 0,
            None
        ).otherwise(col("product_length_cm"))
    )
    .withColumn(
        "product_height_cm",
        when(
            col("product_height_cm") <= 0,
            None
        ).otherwise(col("product_height_cm"))
    )
    .withColumn(
        "product_width_cm",
        when(
            col("product_width_cm") <= 0,
            None
        ).otherwise(col("product_width_cm"))
    )
)


# ============================================================
# INSPECT CLEANED DATA
# ============================================================

print("\nCleaned Product Data:")

products_clean.show(
    10,
    truncate=False
)


# ============================================================
# NULL VALIDATION
# ============================================================

print(
    "NULL product_id:",
    products_clean
    .filter(col("product_id").isNull())
    .count()
)

print(
    "NULL product_category_name:",
    products_clean
    .filter(col("product_category_name").isNull())
    .count()
)

print(
    "NULL product_weight_g:",
    products_clean
    .filter(col("product_weight_g").isNull())
    .count()
)

print(
    "NULL product_length_cm:",
    products_clean
    .filter(col("product_length_cm").isNull())
    .count()
)

print(
    "NULL product_height_cm:",
    products_clean
    .filter(col("product_height_cm").isNull())
    .count()
)

print(
    "NULL product_width_cm:",
    products_clean
    .filter(col("product_width_cm").isNull())
    .count()
)


# ============================================================
# WRITE SILVER DATA
# ============================================================

SILVER_PATH = "data/silver/products"

products_clean.write \
    .mode("overwrite") \
    .parquet(SILVER_PATH)


print("Silver products written successfully.")


spark.stop()