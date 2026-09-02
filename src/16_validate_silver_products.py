from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = (
    SparkSession.builder
    .appName("ValidateSilverProducts")
    .master("local[*]")
    .getOrCreate()
)


SILVER_PATH = "data/silver/products"

products = spark.read.parquet(SILVER_PATH)


# ============================================================
# ROW COUNT
# ============================================================

print(
    "Silver Products Count:",
    products.count()
)


# ============================================================
# SCHEMA
# ============================================================

print("\nSilver Products Schema:")

products.printSchema()


# ============================================================
# SAMPLE DATA
# ============================================================

print("\nSilver Products Sample:")

products.show(
    10,
    truncate=False
)


# ============================================================
# NULL VALIDATION
# ============================================================

print(
    "NULL product_id:",
    products
    .filter(col("product_id").isNull())
    .count()
)

print(
    "NULL product_category_name:",
    products
    .filter(col("product_category_name").isNull())
    .count()
)

print(
    "NULL product_weight_g:",
    products
    .filter(col("product_weight_g").isNull())
    .count()
)

print(
    "NULL product_length_cm:",
    products
    .filter(col("product_length_cm").isNull())
    .count()
)

print(
    "NULL product_height_cm:",
    products
    .filter(col("product_height_cm").isNull())
    .count()
)

print(
    "NULL product_width_cm:",
    products
    .filter(col("product_width_cm").isNull())
    .count()
)


# ============================================================
# DUPLICATE PRODUCT ID VALIDATION
# ============================================================

duplicate_product_ids = (
    products
    .groupBy("product_id")
    .count()
    .filter(col("count") > 1)
)

print(
    "Duplicate product IDs:",
    duplicate_product_ids.count()
)


# ============================================================
# FINISH
# ============================================================

spark.stop()