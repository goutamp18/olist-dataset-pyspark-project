from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, to_timestamp, when


spark = (
    SparkSession.builder
    .appName("OlistSilverOrderReviews")
    .master("local[*]")
    .getOrCreate()
)


BRONZE_PATH = "data/bronze/order_reviews"

reviews = spark.read.parquet(BRONZE_PATH)


# ============================================================
# CLEAN ORDER REVIEW DATA
# ============================================================

reviews_clean = (
    reviews
    .withColumn(
        "review_id",
        trim(col("review_id"))
    )
    .withColumn(
        "order_id",
        trim(col("order_id"))
    )
    .withColumn(
        "review_comment_title",
        trim(col("review_comment_title"))
    )
    .withColumn(
        "review_comment_message",
        trim(col("review_comment_message"))
    )
    .withColumn(
        "review_score",
        when(
            col("review_score").between(1, 5),
            col("review_score")
        ).otherwise(None)
    )
)


# ============================================================
# INSPECT CLEANED DATA
# ============================================================

print("\nCleaned Order Reviews:")

reviews_clean.show(
    10,
    truncate=False
)


# ============================================================
# NULL VALIDATION
# ============================================================

print(
    "NULL review_id:",
    reviews_clean
    .filter(col("review_id").isNull())
    .count()
)

print(
    "NULL order_id:",
    reviews_clean
    .filter(col("order_id").isNull())
    .count()
)

print(
    "NULL review_score:",
    reviews_clean
    .filter(col("review_score").isNull())
    .count()
)

print(
    "NULL review_comment_title:",
    reviews_clean
    .filter(col("review_comment_title").isNull())
    .count()
)

print(
    "NULL review_comment_message:",
    reviews_clean
    .filter(col("review_comment_message").isNull())
    .count()
)

print(
    "NULL review_creation_date:",
    reviews_clean
    .filter(col("review_creation_date").isNull())
    .count()
)

print(
    "NULL review_answer_timestamp:",
    reviews_clean
    .filter(col("review_answer_timestamp").isNull())
    .count()
)


# ============================================================
# REVIEW SCORE VALIDATION
# ============================================================

invalid_review_scores = reviews_clean.filter(
    ~col("review_score").between(1, 5)
    & col("review_score").isNotNull()
)

print(
    "Invalid review scores:",
    invalid_review_scores.count()
)


# ============================================================
# WRITE SILVER DATA
# ============================================================

SILVER_PATH = "data/silver/order_reviews"

reviews_clean.write \
    .mode("overwrite") \
    .parquet(SILVER_PATH)


print("Silver order_reviews written successfully.")


spark.stop()