from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    sum,
    avg,
    round,
    abs
)

spark = (
    SparkSession.builder
    .appName("Validate Gold Category")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

SILVER_PATH = "data/silver"
GOLD_PATH = "data/gold/category"


# ============================================================
# LOAD DATA
# ============================================================

gold_category = spark.read.parquet(GOLD_PATH)

products = spark.read.parquet(
    f"{SILVER_PATH}/products"
)

order_items = spark.read.parquet(
    f"{SILVER_PATH}/order_items"
)

category_translation = spark.read.parquet(
    f"{SILVER_PATH}/category_translation"
)


# ============================================================
# RECREATE SOURCE DATASET
# ============================================================

items_with_products = (
    order_items
    .join(
        products.select(
            "product_id",
            "product_category_name"
        ),
        on="product_id",
        how="left"
    )
)

items_with_category = (
    items_with_products
    .join(
        category_translation,
        on="product_category_name",
        how="left"
    )
)


# ============================================================
# BASIC VALIDATION
# ============================================================

print("\n========== GOLD CATEGORY VALIDATION ==========")

row_count = gold_category.count()

duplicate_categories = (
    gold_category
    .groupBy("product_category_name")
    .count()
    .filter(col("count") > 1)
    .count()
)

negative_revenue = (
    gold_category
    .filter(
        (col("total_product_revenue") < 0) |
        (col("total_freight_revenue") < 0)
    )
    .count()
)

invalid_metrics = (
    gold_category
    .filter(
        (col("unique_products") < 0) |
        (col("total_orders") < 0) |
        (col("total_items_sold") < 0) |
        (col("unique_sellers") < 0) |
        (col("average_item_price") < 0)
    )
    .count()
)

print(f"Category rows: {row_count}")
print(f"Duplicate categories: {duplicate_categories}")
print(f"Negative revenue records: {negative_revenue}")
print(f"Invalid metric records: {invalid_metrics}")


# ============================================================
# EXPECTED CATEGORY COUNT
# ============================================================

expected_category_count = (
    items_with_category
    .select(
        "product_category_name",
        "product_category_name_english"
    )
    .distinct()
    .count()
)

print("\n========== CATEGORY COUNT VALIDATION ==========")

print(
    f"Expected category combinations: "
    f"{expected_category_count}"
)

print(f"Gold category rows: {row_count}")


# ============================================================
# EXPECTED METRICS
# ============================================================

expected_metrics = (
    items_with_category
    .groupBy(
        "product_category_name",
        "product_category_name_english"
    )
    .agg(
        countDistinct("product_id").alias(
            "expected_unique_products"
        ),

        countDistinct("order_id").alias(
            "expected_total_orders"
        ),

        count("*").alias(
            "expected_total_items_sold"
        ),

        countDistinct("seller_id").alias(
            "expected_unique_sellers"
        ),

        round(
            sum("price"),
            2
        ).alias(
            "expected_product_revenue"
        ),

        round(
            sum("freight_value"),
            2
        ).alias(
            "expected_freight_revenue"
        ),

        round(
            avg("price"),
            2
        ).alias(
            "expected_average_item_price"
        )
    )
)


# ============================================================
# JOIN GOLD WITH EXPECTED METRICS
# ============================================================

comparison = (
    gold_category
    .join(
        expected_metrics,
        on=[
            "product_category_name",
            "product_category_name_english"
        ],
        how="left"
    )
)


# ============================================================
# VALIDATE UNIQUE PRODUCTS
# ============================================================

unique_products_mismatch = (
    comparison
    .filter(
        col("unique_products") !=
        col("expected_unique_products")
    )
    .count()
)

print(
    f"Unique products mismatches: "
    f"{unique_products_mismatch}"
)


# ============================================================
# VALIDATE TOTAL ORDERS
# ============================================================

orders_mismatch = (
    comparison
    .filter(
        col("total_orders") !=
        col("expected_total_orders")
    )
    .count()
)

print(
    f"Total orders mismatches: "
    f"{orders_mismatch}"
)


# ============================================================
# VALIDATE TOTAL ITEMS
# ============================================================

items_mismatch = (
    comparison
    .filter(
        col("total_items_sold") !=
        col("expected_total_items_sold")
    )
    .count()
)

print(
    f"Total items sold mismatches: "
    f"{items_mismatch}"
)


# ============================================================
# VALIDATE UNIQUE SELLERS
# ============================================================

sellers_mismatch = (
    comparison
    .filter(
        col("unique_sellers") !=
        col("expected_unique_sellers")
    )
    .count()
)

print(
    f"Unique sellers mismatches: "
    f"{sellers_mismatch}"
)


# ============================================================
# VALIDATE PRODUCT REVENUE
# ============================================================

product_revenue_mismatch = (
    comparison
    .filter(
        abs(
            col("total_product_revenue") -
            col("expected_product_revenue")
        ) > 0.001
    )
    .count()
)

print(
    f"Product revenue mismatches: "
    f"{product_revenue_mismatch}"
)


# ============================================================
# VALIDATE FREIGHT REVENUE
# ============================================================

freight_revenue_mismatch = (
    comparison
    .filter(
        abs(
            col("total_freight_revenue") -
            col("expected_freight_revenue")
        ) > 0.001
    )
    .count()
)

print(
    f"Freight revenue mismatches: "
    f"{freight_revenue_mismatch}"
)


# ============================================================
# VALIDATE AVERAGE ITEM PRICE
# ============================================================

average_price_mismatch = (
    comparison
    .filter(
        abs(
            col("average_item_price") -
            col("expected_average_item_price")
        ) > 0.001
    )
    .count()
)

print(
    f"Average item price mismatches: "
    f"{average_price_mismatch}"
)


# ============================================================
# CHECK SOURCE ROW PRESERVATION
# ============================================================

source_item_count = items_with_category.count()

print("\n========== SOURCE ROW VALIDATION ==========")

print(
    f"Source item rows: {source_item_count}"
)

print(
    f"Gold total items: "
    f"{gold_category.agg(sum('total_items_sold')).first()[0]}"
)


# ============================================================
# SAMPLE
# ============================================================

print("\n========== SAMPLE GOLD CATEGORIES ==========")

gold_category.show(
    20,
    truncate=False
)


# ============================================================
# FINAL
# ============================================================

print("\n========== VALIDATION COMPLETE ==========")

spark.stop()