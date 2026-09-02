from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = (
    SparkSession.builder
    .appName("OlistValidateSilverLayer")
    .master("local[*]")
    .getOrCreate()
)


SILVER_BASE_PATH = "data/silver"


# ============================================================
# DATASETS AND EXPECTED COUNTS
# ============================================================

datasets = {
    "customers": 99441,
    "geolocation": 1000163,
    "order_items": 112650,
    "order_payments": 103886,
    "order_reviews": 104162,
    "orders": 99441,
    "products": 32951,
    "sellers": 3095,
    "category_translation": 71
}


# ============================================================
# LOAD ALL SILVER DATASETS
# ============================================================

silver_data = {}

for dataset_name in datasets:

    path = f"{SILVER_BASE_PATH}/{dataset_name}"

    df = spark.read.parquet(path)

    silver_data[dataset_name] = df

    print("\n" + "=" * 70)
    print(f"DATASET: {dataset_name}")
    print("=" * 70)

    print("Row count:", df.count())

    print("\nSchema:")
    df.printSchema()


# ============================================================
# ROW COUNT VALIDATION
# ============================================================

print("\n\n" + "=" * 70)
print("ROW COUNT VALIDATION")
print("=" * 70)

for dataset_name, expected_count in datasets.items():

    actual_count = silver_data[dataset_name].count()

    status = "PASS" if actual_count == expected_count else "FAIL"

    print(
        f"{dataset_name:25} "
        f"Expected: {expected_count:<10} "
        f"Actual: {actual_count:<10} "
        f"{status}"
    )


# ============================================================
# NULL VALIDATION
# ============================================================

print("\n\n" + "=" * 70)
print("NULL VALIDATION")
print("=" * 70)


critical_columns = {
    "customers": [
        "customer_id",
        "customer_unique_id"
    ],

    "geolocation": [
        "geolocation_zip_code_prefix",
        "geolocation_lat",
        "geolocation_lng",
        "geolocation_city",
        "geolocation_state"
    ],

    "order_items": [
        "order_id",
        "product_id",
        "seller_id"
    ],

    "order_payments": [
        "order_id",
        "payment_type",
        "payment_value"
    ],

    "order_reviews": [
        "review_id"
    ],

    "orders": [
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp"
    ],

    "products": [
        "product_id"
    ],

    "sellers": [
        "seller_id",
        "seller_city",
        "seller_state"
    ],

    "category_translation": [
        "product_category_name",
        "product_category_name_english"
    ]
}


for dataset_name, columns in critical_columns.items():

    df = silver_data[dataset_name]

    print(f"\n{dataset_name}:")

    for column_name in columns:

        null_count = df.filter(
            col(column_name).isNull()
        ).count()

        status = "PASS" if null_count == 0 else "CHECK"

        print(
            f"  {column_name:35} "
            f"NULLs: {null_count:<8} "
            f"{status}"
        )


# ============================================================
# DUPLICATE KEY VALIDATION
# ============================================================

print("\n\n" + "=" * 70)
print("DUPLICATE KEY VALIDATION")
print("=" * 70)


unique_keys = {
    "customers": ["customer_id"],
    "orders": ["order_id"],
    "products": ["product_id"],
    "sellers": ["seller_id"],
    "category_translation": ["product_category_name"]
}


for dataset_name, key_columns in unique_keys.items():

    df = silver_data[dataset_name]

    total_rows = df.count()

    distinct_rows = (
        df.select(*key_columns)
        .distinct()
        .count()
    )

    duplicate_count = total_rows - distinct_rows

    status = "PASS" if duplicate_count == 0 else "CHECK"

    print(
        f"{dataset_name:25} "
        f"Duplicate rows: {duplicate_count:<8} "
        f"{status}"
    )


# ============================================================
# BUSINESS DATA QUALITY VALIDATION
# ============================================================

print("\n\n" + "=" * 70)
print("BUSINESS DATA QUALITY VALIDATION")
print("=" * 70)


# Orders

orders = silver_data["orders"]

invalid_delivery_days = orders.filter(
    col("delivery_days") < 0
).count()

print(
    f"Orders - invalid delivery_days: "
    f"{invalid_delivery_days}"
)


# Order Items

order_items = silver_data["order_items"]

invalid_item_prices = order_items.filter(
    col("price") < 0
).count()

invalid_freight_values = order_items.filter(
    col("freight_value") < 0
).count()

print(
    f"Order Items - negative price: "
    f"{invalid_item_prices}"
)

print(
    f"Order Items - negative freight_value: "
    f"{invalid_freight_values}"
)


# Payments

payments = silver_data["order_payments"]

invalid_payment_values = payments.filter(
    col("payment_value") < 0
).count()

print(
    f"Payments - negative payment_value: "
    f"{invalid_payment_values}"
)


# Products

products = silver_data["products"]

invalid_product_weights = products.filter(
    col("product_weight_g") <= 0
).count()

print(
    f"Products - non-positive weight: "
    f"{invalid_product_weights}"
)


# Geolocation

geolocation = silver_data["geolocation"]

invalid_latitude = geolocation.filter(
    (col("geolocation_lat") < -90) |
    (col("geolocation_lat") > 90)
).count()

invalid_longitude = geolocation.filter(
    (col("geolocation_lng") < -180) |
    (col("geolocation_lng") > 180)
).count()

print(
    f"Geolocation - invalid latitude: "
    f"{invalid_latitude}"
)

print(
    f"Geolocation - invalid longitude: "
    f"{invalid_longitude}"
)


# Reviews

reviews = silver_data["order_reviews"]

invalid_review_scores = reviews.filter(
    col("review_score").isNotNull() &
    (
        (col("review_score") < 1) |
        (col("review_score") > 5)
    )
).count()

print(
    f"Reviews - invalid review_score: "
    f"{invalid_review_scores}"
)


# ============================================================
# SILVER LAYER SUMMARY
# ============================================================

print("\n\n" + "=" * 70)
print("SILVER LAYER VALIDATION COMPLETE")
print("=" * 70)

print("All Silver datasets were successfully loaded and checked.")

print("\nDatasets validated:")

for dataset_name in datasets:
    print(f"  ✓ {dataset_name}")


spark.stop()