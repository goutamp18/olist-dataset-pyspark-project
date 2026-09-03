from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.config import SILVER_DIR, GOLD_DIR


# ============================================================
# Gold Orders
# ============================================================

def run_gold_orders(spark):

    print("Processing Gold dataset: orders")

    orders = spark.read.parquet(
        f"{SILVER_DIR}/orders"
    )

    items = spark.read.parquet(
        f"{SILVER_DIR}/order_items"
    )

    payments = spark.read.parquet(
        f"{SILVER_DIR}/order_payments"
    )

    # --------------------------------------------------------
    # Aggregate order items separately
    # --------------------------------------------------------

    item_metrics = (
        items
        .groupBy("order_id")
        .agg(
            F.round(
                F.sum("price"),
                2
            ).alias("product_revenue"),

            F.round(
                F.sum("freight_value"),
                2
            ).alias("freight_revenue"),

            F.count("*").alias("total_items"),

            F.countDistinct(
                "product_id"
            ).alias("unique_products"),

            F.countDistinct(
                "seller_id"
            ).alias("unique_sellers"),
        )
    )

    # --------------------------------------------------------
    # Aggregate payments separately
    # --------------------------------------------------------

    payment_metrics = (
        payments
        .groupBy("order_id")
        .agg(
            F.round(
                F.sum("payment_value"),
                2
            ).alias("total_payment"),

            F.count("*").alias("payment_count"),
        )
    )

    # --------------------------------------------------------
    # Join aggregated metrics to orders
    # --------------------------------------------------------

    gold_orders = (
        orders

        .join(
            item_metrics,
            on="order_id",
            how="left"
        )

        .join(
            payment_metrics,
            on="order_id",
            how="left"
        )

        .withColumn(
            "product_revenue",
            F.coalesce(
                F.col("product_revenue"),
                F.lit(0.0)
            )
        )

        .withColumn(
            "freight_revenue",
            F.coalesce(
                F.col("freight_revenue"),
                F.lit(0.0)
            )
        )

        .withColumn(
            "total_items",
            F.coalesce(
                F.col("total_items"),
                F.lit(0)
            )
        )

        .withColumn(
            "unique_products",
            F.coalesce(
                F.col("unique_products"),
                F.lit(0)
            )
        )

        .withColumn(
            "unique_sellers",
            F.coalesce(
                F.col("unique_sellers"),
                F.lit(0)
            )
        )

        .withColumn(
            "total_payment",
            F.coalesce(
                F.col("total_payment"),
                F.lit(0.0)
            )
        )

        .withColumn(
            "payment_count",
            F.coalesce(
                F.col("payment_count"),
                F.lit(0)
            )
        )

        .withColumn(
            "total_order_value",
            F.round(
                F.col("product_revenue")
                + F.col("freight_revenue"),
                2
            )
        )
    )

    (
        gold_orders
        .write
        .mode("overwrite")
        .parquet(
            f"{GOLD_DIR}/orders"
        )
    )

    count = gold_orders.count()

    print(
        f"Gold complete: orders | rows={count}"
    )


# ============================================================
# Gold Customers
# ============================================================

def run_gold_customers(spark):

    print("Processing Gold dataset: customers")

    customers = spark.read.parquet(
        f"{SILVER_DIR}/customers"
    )

    orders = spark.read.parquet(
        f"{GOLD_DIR}/orders"
    )

    # --------------------------------------------------------
    # Prepare order metrics
    # --------------------------------------------------------

    orders = (
        orders

        .withColumn(
            "order_value_for_metrics",
            F.coalesce(
                F.col("total_order_value"),
                F.lit(0.0)
            )
        )

        .withColumn(
            "items_for_metrics",
            F.coalesce(
                F.col("total_items"),
                F.lit(0)
            )
        )
    )

    # --------------------------------------------------------
    # Customer-level metrics
    # --------------------------------------------------------

    customer_metrics = (
        orders
        .groupBy("customer_id")
        .agg(
            F.count("*").alias(
                "total_orders"
            ),

            F.round(
                F.sum("order_value_for_metrics"),
                2
            ).alias(
                "total_spent"
            ),

            F.round(
                F.avg("order_value_for_metrics"),
                2
            ).alias(
                "average_order_value"
            ),

            F.sum(
                "items_for_metrics"
            ).alias(
                "total_items_purchased"
            ),

            F.min(
                "order_purchase_timestamp"
            ).alias(
                "first_order_timestamp"
            ),

            F.max(
                "order_purchase_timestamp"
            ).alias(
                "last_order_timestamp"
            ),
        )
    )

    # --------------------------------------------------------
    # Preserve all customers
    # --------------------------------------------------------

    gold_customers = (
        customers

        .join(
            customer_metrics,
            on="customer_id",
            how="left"
        )

        .withColumn(
            "total_orders",
            F.coalesce(
                F.col("total_orders"),
                F.lit(0)
            )
        )

        .withColumn(
            "total_spent",
            F.coalesce(
                F.col("total_spent"),
                F.lit(0.0)
            )
        )

        .withColumn(
            "average_order_value",
            F.coalesce(
                F.col("average_order_value"),
                F.lit(0.0)
            )
        )

        .withColumn(
            "total_items_purchased",
            F.coalesce(
                F.col("total_items_purchased"),
                F.lit(0)
            )
        )
    )

    (
        gold_customers
        .write
        .mode("overwrite")
        .parquet(
            f"{GOLD_DIR}/customers"
        )
    )

    count = gold_customers.count()

    print(
        f"Gold complete: customers | rows={count}"
    )


# ============================================================
# Gold Products
# ============================================================

def run_gold_products(spark):

    print("Processing Gold dataset: products")

    products = spark.read.parquet(
        f"{SILVER_DIR}/products"
    )

    items = spark.read.parquet(
        f"{SILVER_DIR}/order_items"
    )

    # --------------------------------------------------------
    # Product-level metrics
    # --------------------------------------------------------

    product_metrics = (
        items
        .groupBy("product_id")
        .agg(
            F.count("*").alias(
                "total_items_sold"
            ),

            F.countDistinct(
                "order_id"
            ).alias(
                "unique_orders"
            ),

            F.countDistinct(
                "seller_id"
            ).alias(
                "unique_sellers"
            ),

            F.round(
                F.sum("price"),
                2
            ).alias(
                "total_product_revenue"
            ),

            F.round(
                F.sum("freight_value"),
                2
            ).alias(
                "total_freight_revenue"
            ),

            F.round(
                F.avg("price"),
                2
            ).alias(
                "average_item_price"
            ),
        )
    )

    # --------------------------------------------------------
    # Preserve all products
    # --------------------------------------------------------

    gold_products = (
        products

        .join(
            product_metrics,
            on="product_id",
            how="left"
        )

        .withColumn(
            "total_items_sold",
            F.coalesce(
                F.col("total_items_sold"),
                F.lit(0)
            )
        )

        .withColumn(
            "unique_orders",
            F.coalesce(
                F.col("unique_orders"),
                F.lit(0)
            )
        )

        .withColumn(
            "unique_sellers",
            F.coalesce(
                F.col("unique_sellers"),
                F.lit(0)
            )
        )

        .withColumn(
            "total_product_revenue",
            F.coalesce(
                F.col("total_product_revenue"),
                F.lit(0.0)
            )
        )

        .withColumn(
            "total_freight_revenue",
            F.coalesce(
                F.col("total_freight_revenue"),
                F.lit(0.0)
            )
        )

        .withColumn(
            "average_item_price",
            F.coalesce(
                F.col("average_item_price"),
                F.lit(0.0)
            )
        )
    )

    (
        gold_products
        .write
        .mode("overwrite")
        .parquet(
            f"{GOLD_DIR}/products"
        )
    )

    count = gold_products.count()

    print(
        f"Gold complete: products | rows={count}"
    )


# ============================================================
# Gold Sellers
# ============================================================

def run_gold_sellers(spark):

    print("Processing Gold dataset: sellers")

    sellers = spark.read.parquet(
        f"{SILVER_DIR}/sellers"
    )

    items = spark.read.parquet(
        f"{SILVER_DIR}/order_items"
    )

    # --------------------------------------------------------
    # Seller-level metrics
    # --------------------------------------------------------

    seller_metrics = (
        items
        .groupBy("seller_id")
        .agg(
            F.count("*").alias(
                "total_items_sold"
            ),

            F.countDistinct(
                "order_id"
            ).alias(
                "unique_orders"
            ),

            F.countDistinct(
                "product_id"
            ).alias(
                "unique_products"
            ),

            F.round(
                F.sum("price"),
                2
            ).alias(
                "total_product_revenue"
            ),

            F.round(
                F.sum("freight_value"),
                2
            ).alias(
                "total_freight_revenue"
            ),

            F.round(
                F.avg("price"),
                2
            ).alias(
                "average_item_price"
            ),
        )
    )

    # --------------------------------------------------------
    # Preserve all sellers
    # --------------------------------------------------------

    gold_sellers = (
        sellers

        .join(
            seller_metrics,
            on="seller_id",
            how="left"
        )

        .withColumn(
            "total_items_sold",
            F.coalesce(
                F.col("total_items_sold"),
                F.lit(0)
            )
        )

        .withColumn(
            "unique_orders",
            F.coalesce(
                F.col("unique_orders"),
                F.lit(0)
            )
        )

        .withColumn(
            "unique_products",
            F.coalesce(
                F.col("unique_products"),
                F.lit(0)
            )
        )

        .withColumn(
            "total_product_revenue",
            F.coalesce(
                F.col("total_product_revenue"),
                F.lit(0.0)
            )
        )

        .withColumn(
            "total_freight_revenue",
            F.coalesce(
                F.col("total_freight_revenue"),
                F.lit(0.0)
            )
        )

        .withColumn(
            "average_item_price",
            F.coalesce(
                F.col("average_item_price"),
                F.lit(0.0)
            )
        )
    )

    (
        gold_sellers
        .write
        .mode("overwrite")
        .parquet(
            f"{GOLD_DIR}/sellers"
        )
    )

    count = gold_sellers.count()

    print(
        f"Gold complete: sellers | rows={count}"
    )


# ============================================================
# Gold Category
# ============================================================

def run_gold_category(spark):

    print("Processing Gold dataset: category")

    products = spark.read.parquet(
        f"{SILVER_DIR}/products"
    )

    items = spark.read.parquet(
        f"{SILVER_DIR}/order_items"
    )

    translation = spark.read.parquet(
        f"{SILVER_DIR}/category_translation"
    )

    # --------------------------------------------------------
    # Attach product category to order items
    # --------------------------------------------------------

    item_products = (
        items
        .join(
            products.select(
                "product_id",
                "product_category_name"
            ),
            on="product_id",
            how="left"
        )
    )

    # --------------------------------------------------------
    # Category-level metrics
    # --------------------------------------------------------

    category_metrics = (
        item_products
        .groupBy(
            "product_category_name"
        )
        .agg(
            F.count("*").alias(
                "total_items_sold"
            ),

            F.countDistinct(
                "order_id"
            ).alias(
                "unique_orders"
            ),

            F.countDistinct(
                "product_id"
            ).alias(
                "unique_products"
            ),

            F.round(
                F.sum("price"),
                2
            ).alias(
                "total_product_revenue"
            ),

            F.round(
                F.sum("freight_value"),
                2
            ).alias(
                "total_freight_revenue"
            ),

            F.round(
                F.avg("price"),
                2
            ).alias(
                "average_item_price"
            ),
        )
    )

    # --------------------------------------------------------
    # Add English category name
    # --------------------------------------------------------

    gold_category = (
        category_metrics
        .join(
            translation,
            on="product_category_name",
            how="left"
        )
    )

    (
        gold_category
        .write
        .mode("overwrite")
        .parquet(
            f"{GOLD_DIR}/category"
        )
    )

    count = gold_category.count()

    print(
        f"Gold complete: category | rows={count}"
    )


# ============================================================
# Gold Reviews
# ============================================================

def run_gold_reviews(spark):

    print("Processing Gold dataset: reviews")

    reviews = spark.read.parquet(
        f"{SILVER_DIR}/order_reviews"
    )

    # --------------------------------------------------------
    # Gold Reviews is an order-level analytical table.
    #
    # Reviews without order_id cannot be associated
    # with an order, so remove them before aggregation.
    # --------------------------------------------------------

    reviews = (
        reviews
        .filter(
            F.col("order_id").isNotNull()
        )
    )

    # --------------------------------------------------------
    # Calculate response time
    # --------------------------------------------------------

    reviews = (
        reviews
        .withColumn(
            "review_response_hours",
            F.when(
                F.col(
                    "review_creation_date"
                ).isNotNull()
                &
                F.col(
                    "review_answer_timestamp"
                ).isNotNull(),

                (
                    F.unix_timestamp(
                        "review_answer_timestamp"
                    )
                    -
                    F.unix_timestamp(
                        "review_creation_date"
                    )
                ) / 3600.0
            )
        )
    )

    # --------------------------------------------------------
    # Aggregate reviews by order
    # --------------------------------------------------------

    review_metrics = (
        reviews
        .groupBy("order_id")
        .agg(
            F.count("*").alias(
                "review_count"
            ),

            F.round(
                F.avg("review_score"),
                2
            ).alias(
                "average_review_score"
            ),

            F.round(
                F.avg("review_response_hours"),
                2
            ).alias(
                "average_response_hours"
            ),
        )
    )

    gold_reviews = review_metrics

    (
        gold_reviews
        .write
        .mode("overwrite")
        .parquet(
            f"{GOLD_DIR}/reviews"
        )
    )

    count = gold_reviews.count()

    print(
        f"Gold complete: reviews | rows={count}"
    )


# ============================================================
# Gold Payments
# ============================================================

def run_gold_payments(spark):

    print("Processing Gold dataset: payments")

    payments = spark.read.parquet(
        f"{SILVER_DIR}/order_payments"
    )

    # --------------------------------------------------------
    # Payment-level aggregation by order
    # --------------------------------------------------------

    payment_metrics = (
        payments
        .groupBy("order_id")
        .agg(
            F.count("*").alias(
                "payment_count"
            ),

            F.round(
                F.sum("payment_value"),
                2
            ).alias(
                "total_payment_value"
            ),

            F.round(
                F.avg("payment_value"),
                2
            ).alias(
                "average_payment_value"
            ),

            F.sum(
                "payment_installments"
            ).alias(
                "total_installments"
            ),

            F.max(
                "payment_installments"
            ).alias(
                "max_installments"
            ),

            F.countDistinct(
                "payment_type"
            ).alias(
                "unique_payment_types"
            ),
        )
    )

    gold_payments = payment_metrics

    (
        gold_payments
        .write
        .mode("overwrite")
        .parquet(
            f"{GOLD_DIR}/payments"
        )
    )

    count = gold_payments.count()

    print(
        f"Gold complete: payments | rows={count}"
    )


# ============================================================
# Gold Geolocation
# ============================================================

def run_gold_geolocation(spark):

    print("Processing Gold dataset: geolocation")

    geolocation = spark.read.parquet(
        f"{SILVER_DIR}/geolocation"
    )

    # --------------------------------------------------------
    # Aggregate geolocation records by ZIP prefix
    # --------------------------------------------------------

    gold_geolocation = (
        geolocation
        .groupBy(
            "geolocation_zip_code_prefix"
        )
        .agg(
            F.round(
                F.avg("geolocation_lat"),
                6
            ).alias(
                "average_latitude"
            ),

            F.round(
                F.avg("geolocation_lng"),
                6
            ).alias(
                "average_longitude"
            ),

            F.first(
                "geolocation_city",
                ignorenulls=True
            ).alias(
                "city"
            ),

            F.first(
                "geolocation_state",
                ignorenulls=True
            ).alias(
                "state"
            ),
        )
    )

    (
        gold_geolocation
        .write
        .mode("overwrite")
        .parquet(
            f"{GOLD_DIR}/geolocation"
        )
    )

    count = gold_geolocation.count()

    print(
        f"Gold complete: geolocation | rows={count}"
    )


# ============================================================
# Run Complete Gold Layer
# ============================================================

def run_gold(spark):

    print("=" * 60)
    print("Starting Gold layer")
    print("=" * 60)

    run_gold_orders(spark)
    run_gold_customers(spark)
    run_gold_products(spark)
    run_gold_sellers(spark)
    run_gold_category(spark)
    run_gold_reviews(spark)
    run_gold_payments(spark)
    run_gold_geolocation(spark)

    print("=" * 60)
    print("Gold layer completed successfully.")
    print("=" * 60)