from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

spark = (
    SparkSession.builder
    .appName("OlistRelationshipValidation")
    .master("local[*]")
    .getOrCreate()
)

base_path = "data/raw"

orders = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{base_path}/olist_orders_dataset.csv")
)

order_items = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{base_path}/olist_order_items_dataset.csv")
)

payments = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{base_path}/olist_order_payments_dataset.csv")
)

customers = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{base_path}/olist_customers_dataset.csv")
)

products = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{base_path}/olist_products_dataset.csv")
)

sellers = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{base_path}/olist_sellers_dataset.csv")
)


# ============================================================
# 1. ORDERS → CUSTOMERS
# ============================================================

print("\n========== ORDERS → CUSTOMERS ==========")

order_customer = (
    orders
    .join(customers, "customer_id", "left")
)

print("Orders:", orders.count())
print("After customer join:", order_customer.count())


# ============================================================
# 2. ORDERS → ORDER ITEMS
# ============================================================

print("\n========== ORDERS → ORDER ITEMS ==========")

order_item_counts = (
    order_items
    .groupBy("order_id")
    .agg(count("*").alias("item_count"))
)

order_item_counts.orderBy(
    col("item_count").desc()
).show(10)


print("Orders:", orders.count())
print("Order items:", order_items.count())

orders_with_items = (
    orders
    .join(order_items, "order_id", "left")
)

print("After orders → items join:", orders_with_items.count())


# ============================================================
# 3. ORDERS → PAYMENTS
# ============================================================

print("\n========== ORDERS → PAYMENTS ==========")

payment_counts = (
    payments
    .groupBy("order_id")
    .agg(count("*").alias("payment_count"))
)

payment_counts.orderBy(
    col("payment_count").desc()
).show(10)


print("Orders:", orders.count())
print("Payments:", payments.count())

orders_with_payments = (
    orders
    .join(payments, "order_id", "left")
)

print("After orders → payments join:", orders_with_payments.count())


# ============================================================
# 4. ORDER ITEMS → PRODUCTS
# ============================================================

print("\n========== ORDER ITEMS → PRODUCTS ==========")

order_product = (
    order_items
    .join(products, "product_id", "left")
)

print("Order items:", order_items.count())
print("After product join:", order_product.count())


# ============================================================
# 5. ORDER ITEMS → SELLERS
# ============================================================

print("\n========== ORDER ITEMS → SELLERS ==========")

order_seller = (
    order_items
    .join(sellers, "seller_id", "left")
)

print("Order items:", order_items.count())
print("After seller join:", order_seller.count())


# ============================================================
# 6. ORPHAN RECORD CHECKS
# ============================================================

print("\n========== ORPHAN ORDER ITEMS ==========")

orphan_items = (
    order_items
    .join(
        orders.select("order_id"),
        "order_id",
        "left_anti"
    )
)

print("Order items without matching order:", orphan_items.count())


print("\n========== ORPHAN PAYMENTS ==========")

orphan_payments = (
    payments
    .join(
        orders.select("order_id"),
        "order_id",
        "left_anti"
    )
)

print("Payments without matching order:", orphan_payments.count())


print("\n========== ORPHAN PRODUCTS ==========")

orphan_products = (
    order_items
    .join(
        products.select("product_id"),
        "product_id",
        "left_anti"
    )
)

print("Order items without matching product:", orphan_products.count())


print("\n========== ORPHAN SELLERS ==========")

orphan_sellers = (
    order_items
    .join(
        sellers.select("seller_id"),
        "seller_id",
        "left_anti"
    )
)

print("Order items without matching seller:", orphan_sellers.count())


spark.stop()