from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.config import BRONZE_DIR, SILVER_DIR


def run_silver(spark: SparkSession):
    print("=" * 60)
    print("Starting Silver layer")
    print("=" * 60)

    # ---------------------------------------------------------
    # Orders
    # ---------------------------------------------------------
    print("Processing Silver dataset: orders")

    orders = (
        spark.read.parquet(str(BRONZE_DIR / "orders"))
        .withColumn("order_id", F.trim(F.col("order_id")))
        .withColumn("customer_id", F.trim(F.col("customer_id")))
        .withColumn("order_status", F.lower(F.trim(F.col("order_status"))))
        .withColumn(
            "delivery_days",
            F.when(
                F.col("order_delivered_customer_date").isNotNull(),
                F.datediff(
                    F.to_date("order_delivered_customer_date"),
                    F.to_date("order_purchase_timestamp"),
                ),
            ),
        )
        .withColumn(
            "is_delivered",
            F.when(F.col("order_status") == "delivered", True).otherwise(False),
        )
    )

    (
        orders.write
        .mode("overwrite")
        .parquet(str(SILVER_DIR / "orders"))
    )

    print(f"Silver complete: orders | rows={orders.count()}")

    # ---------------------------------------------------------
    # Customers
    # ---------------------------------------------------------
    print("Processing Silver dataset: customers")

    customers = (
        spark.read.parquet(str(BRONZE_DIR / "customers"))
        .withColumn("customer_id", F.trim(F.col("customer_id")))
        .withColumn("customer_unique_id", F.trim(F.col("customer_unique_id")))
        .withColumn("customer_city", F.trim(F.col("customer_city")))
        .withColumn("customer_state", F.upper(F.trim(F.col("customer_state"))))
    )

    (
        customers.write
        .mode("overwrite")
        .parquet(str(SILVER_DIR / "customers"))
    )

    print(f"Silver complete: customers | rows={customers.count()}")

    # ---------------------------------------------------------
    # Order Items
    # ---------------------------------------------------------
    print("Processing Silver dataset: order_items")

    order_items = (
        spark.read.parquet(str(BRONZE_DIR / "order_items"))
        .withColumn("order_id", F.trim(F.col("order_id")))
        .withColumn("product_id", F.trim(F.col("product_id")))
        .withColumn("seller_id", F.trim(F.col("seller_id")))
        .withColumn("price", F.round(F.col("price"), 2))
        .withColumn("freight_value", F.round(F.col("freight_value"), 2))
    )

    (
        order_items.write
        .mode("overwrite")
        .parquet(str(SILVER_DIR / "order_items"))
    )

    print(f"Silver complete: order_items | rows={order_items.count()}")

    # ---------------------------------------------------------
    # Order Payments
    # ---------------------------------------------------------
    print("Processing Silver dataset: order_payments")

    payments = (
        spark.read.parquet(str(BRONZE_DIR / "order_payments"))
        .withColumn("order_id", F.trim(F.col("order_id")))
        .withColumn("payment_type", F.lower(F.trim(F.col("payment_type"))))
        .withColumn(
            "payment_installments",
            F.when(
                F.col("payment_installments") > 0,
                F.col("payment_installments"),
            ),
        )
        .withColumn("payment_value", F.round(F.col("payment_value"), 2))
    )

    (
        payments.write
        .mode("overwrite")
        .parquet(str(SILVER_DIR / "order_payments"))
    )

    print(f"Silver complete: order_payments | rows={payments.count()}")

    # ---------------------------------------------------------
    # Products
    # ---------------------------------------------------------
    print("Processing Silver dataset: products")

    products = (
        spark.read.parquet(str(BRONZE_DIR / "products"))
        .withColumn("product_id", F.trim(F.col("product_id")))
        .withColumn(
            "product_category_name",
            F.lower(F.trim(F.col("product_category_name"))),
        )
        .withColumn(
            "product_weight_g",
            F.when(F.col("product_weight_g") > 0, F.col("product_weight_g")),
        )
        .withColumn(
            "product_length_cm",
            F.when(F.col("product_length_cm") > 0, F.col("product_length_cm")),
        )
        .withColumn(
            "product_height_cm",
            F.when(F.col("product_height_cm") > 0, F.col("product_height_cm")),
        )
        .withColumn(
            "product_width_cm",
            F.when(F.col("product_width_cm") > 0, F.col("product_width_cm")),
        )
    )

    (
        products.write
        .mode("overwrite")
        .parquet(str(SILVER_DIR / "products"))
    )

    print(f"Silver complete: products | rows={products.count()}")

    # ---------------------------------------------------------
    # Sellers
    # ---------------------------------------------------------
    print("Processing Silver dataset: sellers")

    sellers = (
        spark.read.parquet(str(BRONZE_DIR / "sellers"))
        .withColumn("seller_id", F.trim(F.col("seller_id")))
        .withColumn("seller_city", F.trim(F.col("seller_city")))
        .withColumn("seller_state", F.upper(F.trim(F.col("seller_state"))))
    )

    (
        sellers.write
        .mode("overwrite")
        .parquet(str(SILVER_DIR / "sellers"))
    )

    print(f"Silver complete: sellers | rows={sellers.count()}")

        # ---------------------------------------------------------
    # Order Reviews
    # ---------------------------------------------------------
    print("Processing Silver dataset: order_reviews")

    reviews = (
        spark.read.parquet(str(BRONZE_DIR / "order_reviews"))
        .withColumn("review_id", F.trim(F.col("review_id")))
        .withColumn("order_id", F.trim(F.col("order_id")))
        .withColumn(
            "review_comment_title",
            F.trim(F.col("review_comment_title")),
        )
        .withColumn(
            "review_comment_message",
            F.trim(F.col("review_comment_message")),
        )
        .withColumn(
            "review_score",
            F.when(
                F.col("review_score").between(1, 5),
                F.col("review_score"),
            ),
        )
    )

    (
        reviews.write
        .mode("overwrite")
        .parquet(str(SILVER_DIR / "order_reviews"))
    )

    print(f"Silver complete: order_reviews | rows={reviews.count()}")

    # ---------------------------------------------------------
    # Geolocation
    # ---------------------------------------------------------
    print("Processing Silver dataset: geolocation")

    geolocation = (
        spark.read.parquet(str(BRONZE_DIR / "geolocation"))
        .withColumn(
            "geolocation_city",
            F.trim(F.col("geolocation_city")),
        )
        .withColumn(
            "geolocation_state",
            F.upper(F.trim(F.col("geolocation_state"))),
        )
    )

    (
        geolocation.write
        .mode("overwrite")
        .parquet(str(SILVER_DIR / "geolocation"))
    )

    print(
        f"Silver complete: geolocation | "
        f"rows={geolocation.count()}"
    )
    
    # ---------------------------------------------------------
    # Category Translation
    # ---------------------------------------------------------
    print("Processing Silver dataset: category_translation")

    category_translation = (
        spark.read.parquet(str(BRONZE_DIR / "category_translation"))
        .withColumn(
            "product_category_name",
            F.lower(F.trim(F.col("product_category_name"))),
        )
        .withColumn(
            "product_category_name_english",
            F.lower(F.trim(F.col("product_category_name_english"))),
        )
    )

    (
        category_translation.write
        .mode("overwrite")
        .parquet(str(SILVER_DIR / "category_translation"))
    )

    print(
        f"Silver complete: category_translation | "
        f"rows={category_translation.count()}"
    )

    print("=" * 60)
    print("Silver layer completed successfully.")
    print("=" * 60)