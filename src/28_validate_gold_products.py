from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, round, abs

spark = (
    SparkSession.builder
    .appName("Validate Gold Products")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

GOLD_PATH = "data/gold/products"

products = spark.read.parquet(GOLD_PATH)

print("\n========== GOLD PRODUCTS VALIDATION ==========")

# 1. Row count
row_count = products.count()
print(f"Row count: {row_count}")

# 2. Distinct product IDs
distinct_products = products.select("product_id").distinct().count()
print(f"Distinct product IDs: {distinct_products}")

# 3. Duplicate product IDs
duplicate_products = (
    products
    .groupBy("product_id")
    .agg(count("*").alias("count"))
    .filter(col("count") > 1)
    .count()
)

print(f"Duplicate product IDs: {duplicate_products}")

# 4. NULL product IDs
null_product_ids = (
    products
    .filter(col("product_id").isNull())
    .count()
)

print(f"NULL product IDs: {null_product_ids}")

# 5. Invalid order counts
invalid_orders = (
    products
    .filter(col("total_orders") < 0)
    .count()
)

print(f"Invalid total_orders: {invalid_orders}")

# 6. Invalid item counts
invalid_items = (
    products
    .filter(col("total_items_sold") < 0)
    .count()
)

print(f"Invalid total_items_sold: {invalid_items}")

# 7. Invalid seller counts
invalid_sellers = (
    products
    .filter(col("unique_sellers") < 0)
    .count()
)

print(f"Invalid unique_sellers: {invalid_sellers}")

# 8. Negative revenue
negative_revenue = (
    products
    .filter(
        (col("total_product_revenue") < 0) |
        (col("total_freight_revenue") < 0)
    )
    .count()
)

print(f"Negative revenue records: {negative_revenue}")

# 9. Negative average price
negative_average_price = (
    products
    .filter(col("average_item_price") < 0)
    .count()
)

print(f"Negative average_item_price: {negative_average_price}")

# 10. Average price validation
# For products with sales:
# total_product_revenue / total_items_sold
# should equal average_item_price after rounding.
silver_order_items = spark.read.parquet("data/silver/order_items")

expected_average = (
    silver_order_items
    .groupBy("product_id")
    .agg(
        round(
            # average based on original price values
            __import__("pyspark.sql.functions", fromlist=["avg"]).avg("price"),
            2
        ).alias("expected_average_item_price")
    )
)

average_mismatch = (
    products
    .join(expected_average, "product_id", "left")
    .filter(
        abs(
            col("average_item_price") -
            col("expected_average_item_price")
        ) > 0.001
    )
    .count()
)

print(f"Average price mismatches: {average_mismatch}")
print(f"Average price mismatches: {average_mismatch}")

# 11. Products with no sales
no_sales = (
    products
    .filter(col("total_items_sold") == 0)
    .count()
)

print(f"Products with no sales: {no_sales}")

# 12. Revenue summary
print("\n========== REVENUE SUMMARY ==========")

products.select(
    "total_product_revenue",
    "total_freight_revenue",
    "average_item_price"
).describe().show()

# 13. Sample
print("\n========== SAMPLE GOLD PRODUCTS ==========")

products.show(10, truncate=False)

print("\n========== VALIDATION COMPLETE ==========")

spark.stop()