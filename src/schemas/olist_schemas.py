from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType
)


# ============================================================
# CUSTOMERS
# ============================================================

customers_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("customer_unique_id", StringType(), False),
    StructField("customer_zip_code_prefix", IntegerType(), False),
    StructField("customer_city", StringType(), False),
    StructField("customer_state", StringType(), False)
])


# ============================================================
# GEOLOCATION
# ============================================================

geolocation_schema = StructType([
    StructField("geolocation_zip_code_prefix", IntegerType(), False),
    StructField("geolocation_lat", DoubleType(), False),
    StructField("geolocation_lng", DoubleType(), False),
    StructField("geolocation_city", StringType(), False),
    StructField("geolocation_state", StringType(), False)
])


# ============================================================
# ORDER ITEMS
# ============================================================

order_items_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("order_item_id", IntegerType(), False),
    StructField("product_id", StringType(), False),
    StructField("seller_id", StringType(), False),
    StructField("shipping_limit_date", TimestampType(), False),
    StructField("price", DoubleType(), False),
    StructField("freight_value", DoubleType(), False)
])


# ============================================================
# ORDER PAYMENTS
# ============================================================

order_payments_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("payment_sequential", IntegerType(), False),
    StructField("payment_type", StringType(), False),
    StructField("payment_installments", IntegerType(), False),
    StructField("payment_value", DoubleType(), False)
])


# ============================================================
# ORDER REVIEWS
# ============================================================

order_reviews_schema = StructType([
    StructField("review_id", StringType(), False),
    StructField("order_id", StringType(), False),
    StructField("review_score", IntegerType(), True),
    StructField("review_comment_title", StringType(), True),
    StructField("review_comment_message", StringType(), True),
    StructField("review_creation_date", TimestampType(), True),
    StructField("review_answer_timestamp", TimestampType(), True)
])


# ============================================================
# ORDERS
# ============================================================

orders_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("order_status", StringType(), False),
    StructField("order_purchase_timestamp", TimestampType(), False),
    StructField("order_approved_at", TimestampType(), True),
    StructField("order_delivered_carrier_date", TimestampType(), True),
    StructField("order_delivered_customer_date", TimestampType(), True),
    StructField("order_estimated_delivery_date", TimestampType(), False)
])


# ============================================================
# PRODUCTS
# ============================================================

products_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_category_name", StringType(), True),
    StructField("product_name_lenght", IntegerType(), True),
    StructField("product_description_lenght", IntegerType(), True),
    StructField("product_photos_qty", IntegerType(), True),
    StructField("product_weight_g", IntegerType(), True),
    StructField("product_length_cm", IntegerType(), True),
    StructField("product_height_cm", IntegerType(), True),
    StructField("product_width_cm", IntegerType(), True)
])


# ============================================================
# SELLERS
# ============================================================

sellers_schema = StructType([
    StructField("seller_id", StringType(), False),
    StructField("seller_zip_code_prefix", IntegerType(), False),
    StructField("seller_city", StringType(), False),
    StructField("seller_state", StringType(), False)
])


# ============================================================
# CATEGORY TRANSLATION
# ============================================================

category_translation_schema = StructType([
    StructField("product_category_name", StringType(), False),
    StructField("product_category_name_english", StringType(), False)
])