from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.config import GOLD_DIR


# ============================================================
# Spark Session
# ============================================================

def create_spark_session():
    return (
        SparkSession.builder
        .appName("OlistGoldValidation")
        .master("local[*]")
        .getOrCreate()
    )


# ============================================================
# Helper Functions
# ============================================================

def print_result(check_name, passed, details=""):
    status = "PASS" if passed else "FAIL"

    print(
        f"[{status}] {check_name}"
        + (f" | {details}" if details else "")
    )

    return passed


def check_row_count(df, expected, name):
    actual = df.count()

    return print_result(
        f"{name} row count",
        actual == expected,
        f"expected={expected}, actual={actual}"
    )


def check_duplicate_keys(df, key, name):
    duplicates = (
        df.groupBy(key)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    return print_result(
        f"{name} duplicate {key}",
        duplicates == 0,
        f"duplicates={duplicates}"
    )


def check_null_key(df, key, name):
    nulls = df.filter(F.col(key).isNull()).count()

    return print_result(
        f"{name} null {key}",
        nulls == 0,
        f"nulls={nulls}"
    )


def check_negative_values(df, column, name):
    negative = (
        df.filter(F.col(column) < 0)
        .count()
    )

    return print_result(
        f"{name} negative {column}",
        negative == 0,
        f"negative_rows={negative}"
    )


# ============================================================
# Load Gold Data
# ============================================================

def load_gold_data(spark):

    gold = {}

    datasets = [
        "orders",
        "customers",
        "products",
        "sellers",
        "category",
        "reviews",
        "payments",
        "geolocation",
    ]

    for dataset in datasets:
        print(f"Loading Gold dataset: {dataset}")

        gold[dataset] = spark.read.parquet(
            f"{GOLD_DIR}/{dataset}"
        )

    return gold


# ============================================================
# Validate Gold Orders
# ============================================================

def validate_orders(df):

    print("\n" + "=" * 60)
    print("VALIDATING GOLD ORDERS")
    print("=" * 60)

    results = []

    results.append(
        check_row_count(
            df,
            99441,
            "orders"
        )
    )

    results.append(
        check_duplicate_keys(
            df,
            "order_id",
            "orders"
        )
    )

    results.append(
        check_null_key(
            df,
            "order_id",
            "orders"
        )
    )

    results.append(
        check_negative_values(
            df,
            "product_revenue",
            "orders"
        )
    )

    results.append(
        check_negative_values(
            df,
            "freight_revenue",
            "orders"
        )
    )

    results.append(
        check_negative_values(
            df,
            "total_order_value",
            "orders"
        )
    )

    # Validate total_order_value
    invalid_total = (
        df.filter(
            F.round(
                F.col("product_revenue")
                + F.col("freight_revenue"),
                2
            )
            != F.col("total_order_value")
        )
        .count()
    )

    results.append(
        print_result(
            "orders total_order_value calculation",
            invalid_total == 0,
            f"invalid_rows={invalid_total}"
        )
    )

    return results


# ============================================================
# Validate Gold Customers
# ============================================================

def validate_customers(df):

    print("\n" + "=" * 60)
    print("VALIDATING GOLD CUSTOMERS")
    print("=" * 60)

    results = []

    results.append(
        check_row_count(
            df,
            99441,
            "customers"
        )
    )

    results.append(
        check_duplicate_keys(
            df,
            "customer_id",
            "customers"
        )
    )

    results.append(
        check_null_key(
            df,
            "customer_id",
            "customers"
        )
    )

    for column in [
        "total_orders",
        "total_spent",
        "average_order_value",
        "total_items_purchased",
    ]:
        results.append(
            check_negative_values(
                df,
                column,
                "customers"
            )
        )

    # Average order value validation
    invalid_avg = (
        df.filter(
            (
                F.col("total_orders") > 0
            )
            &
            (
                F.abs(
                    F.col("average_order_value")
                    -
                    (
                        F.col("total_spent")
                        /
                        F.col("total_orders")
                    )
                ) > 0.01
            )
        )
        .count()
    )

    results.append(
        print_result(
            "customers average_order_value",
            invalid_avg == 0,
            f"invalid_rows={invalid_avg}"
        )
    )

    return results


# ============================================================
# Validate Gold Products
# ============================================================

def validate_products(df):

    print("\n" + "=" * 60)
    print("VALIDATING GOLD PRODUCTS")
    print("=" * 60)

    results = []

    results.append(
        check_row_count(
            df,
            32951,
            "products"
        )
    )

    results.append(
        check_duplicate_keys(
            df,
            "product_id",
            "products"
        )
    )

    results.append(
        check_null_key(
            df,
            "product_id",
            "products"
        )
    )

    for column in [
        "total_items_sold",
        "unique_orders",
        "unique_sellers",
        "total_product_revenue",
        "total_freight_revenue",
        "average_item_price",
    ]:
        results.append(
            check_negative_values(
                df,
                column,
                "products"
            )
        )

    return results


# ============================================================
# Validate Gold Sellers
# ============================================================

def validate_sellers(df):

    print("\n" + "=" * 60)
    print("VALIDATING GOLD SELLERS")
    print("=" * 60)

    results = []

    results.append(
        check_row_count(
            df,
            3095,
            "sellers"
        )
    )

    results.append(
        check_duplicate_keys(
            df,
            "seller_id",
            "sellers"
        )
    )

    results.append(
        check_null_key(
            df,
            "seller_id",
            "sellers"
        )
    )

    for column in [
        "total_items_sold",
        "unique_orders",
        "unique_products",
        "total_product_revenue",
        "total_freight_revenue",
        "average_item_price",
    ]:
        results.append(
            check_negative_values(
                df,
                column,
                "sellers"
            )
        )

    return results


# ============================================================
# Validate Gold Category
# ============================================================

def validate_category(df):

    print("\n" + "=" * 60)
    print("VALIDATING GOLD CATEGORY")
    print("=" * 60)

    results = []

    results.append(
        check_row_count(
            df,
            74,
            "category"
        )
    )

    for column in [
        "total_items_sold",
        "unique_orders",
        "unique_products",
        "total_product_revenue",
        "total_freight_revenue",
        "average_item_price",
    ]:
        results.append(
            check_negative_values(
                df,
                column,
                "category"
            )
        )

    return results


# ============================================================
# Validate Gold Reviews
# ============================================================

def validate_reviews(df):

    print("\n" + "=" * 60)
    print("VALIDATING GOLD REVIEWS")
    print("=" * 60)

    results = []

    results.append(
        check_duplicate_keys(
            df,
            "order_id",
            "reviews"
        )
    )

    results.append(
        check_null_key(
            df,
            "order_id",
            "reviews"
        )
    )

    results.append(
        check_negative_values(
            df,
            "review_count",
            "reviews"
        )
    )

    results.append(
        check_negative_values(
            df,
            "average_response_hours",
            "reviews"
        )
    )

    # Review score must be between 1 and 5
    invalid_scores = (
        df.filter(
            (
                F.col("average_review_score") < 1
            )
            |
            (
                F.col("average_review_score") > 5
            )
        )
        .count()
    )

    results.append(
        print_result(
            "reviews average score range",
            invalid_scores == 0,
            f"invalid_rows={invalid_scores}"
        )
    )

    return results


# ============================================================
# Validate Gold Payments
# ============================================================

def validate_payments(df):

    print("\n" + "=" * 60)
    print("VALIDATING GOLD PAYMENTS")
    print("=" * 60)

    results = []

    results.append(
        check_duplicate_keys(
            df,
            "order_id",
            "payments"
        )
    )

    results.append(
        check_null_key(
            df,
            "order_id",
            "payments"
        )
    )

    for column in [
        "payment_count",
        "total_payment_value",
        "average_payment_value",
        "total_installments",
        "max_installments",
        "unique_payment_types",
    ]:
        results.append(
            check_negative_values(
                df,
                column,
                "payments"
            )
        )

    return results


# ============================================================
# Validate Gold Geolocation
# ============================================================

def validate_geolocation(df):

    print("\n" + "=" * 60)
    print("VALIDATING GOLD GEOLOCATION")
    print("=" * 60)

    results = []

    results.append(
        check_duplicate_keys(
            df,
            "geolocation_zip_code_prefix",
            "geolocation"
        )
    )

    results.append(
        check_null_key(
            df,
            "geolocation_zip_code_prefix",
            "geolocation"
        )
    )

    # Latitude validation
    invalid_lat = (
        df.filter(
            (F.col("average_latitude") < -90)
            |
            (F.col("average_latitude") > 90)
        )
        .count()
    )

    results.append(
        print_result(
            "geolocation latitude range",
            invalid_lat == 0,
            f"invalid_rows={invalid_lat}"
        )
    )

    # Longitude validation
    invalid_lng = (
        df.filter(
            (F.col("average_longitude") < -180)
            |
            (F.col("average_longitude") > 180)
        )
        .count()
    )

    results.append(
        print_result(
            "geolocation longitude range",
            invalid_lng == 0,
            f"invalid_rows={invalid_lng}"
        )
    )

    return results


# ============================================================
# Referential Integrity
# ============================================================

def validate_referential_integrity(gold):

    print("\n" + "=" * 60)
    print("VALIDATING REFERENTIAL INTEGRITY")
    print("=" * 60)

    results = []

    orders = gold["orders"]
    customers = gold["customers"]
    products = gold["products"]
    sellers = gold["sellers"]

    # Orders → Customers
    missing_customers = (
        orders
        .select("customer_id")
        .distinct()
        .join(
            customers.select("customer_id").distinct(),
            on="customer_id",
            how="left_anti"
        )
        .count()
    )

    results.append(
        print_result(
            "orders → customers",
            missing_customers == 0,
            f"missing_customers={missing_customers}"
        )
    )

    return results


# ============================================================
# Revenue Reconciliation
# ============================================================

def validate_revenue(gold):

    print("\n" + "=" * 60)
    print("VALIDATING REVENUE RECONCILIATION")
    print("=" * 60)

    orders = gold["orders"]

    order_revenue = (
        orders
        .agg(
            F.round(
                F.sum("product_revenue"),
                2
            ).alias("product_revenue"),

            F.round(
                F.sum("freight_revenue"),
                2
            ).alias("freight_revenue"),

            F.round(
                F.sum("total_order_value"),
                2
            ).alias("total_order_value"),

            F.round(
                F.sum("total_payment"),
                2
            ).alias("total_payment")
        )
        .collect()[0]
    )

    expected_total = round(
        float(order_revenue["product_revenue"])
        +
        float(order_revenue["freight_revenue"]),
        2
    )

    actual_total = float(
        order_revenue["total_order_value"]
    )

    passed = abs(
        expected_total - actual_total
    ) <= 0.01

    print_result(
        "product + freight = total order value",
        passed,
        (
            f"product_revenue={order_revenue['product_revenue']}, "
            f"freight_revenue={order_revenue['freight_revenue']}, "
            f"total_order_value={actual_total}"
        )
    )

    print(
        f"Total payment value: "
        f"{order_revenue['total_payment']}"
    )

    print(
        "Note: total payment is not required "
        "to equal total order value."
    )

    return [passed]


# ============================================================
# Main Validation
# ============================================================

def main():

    spark = create_spark_session()

    print("=" * 60)
    print("GOLD DATA QUALITY VALIDATION")
    print("=" * 60)

    print(f"Gold directory: {GOLD_DIR}")

    gold = load_gold_data(spark)

    results = []

    results.extend(
        validate_orders(
            gold["orders"]
        )
    )

    results.extend(
        validate_customers(
            gold["customers"]
        )
    )

    results.extend(
        validate_products(
            gold["products"]
        )
    )

    results.extend(
        validate_sellers(
            gold["sellers"]
        )
    )

    results.extend(
        validate_category(
            gold["category"]
        )
    )

    results.extend(
        validate_reviews(
            gold["reviews"]
        )
    )

    results.extend(
        validate_payments(
            gold["payments"]
        )
    )

    results.extend(
        validate_geolocation(
            gold["geolocation"]
        )
    )

    results.extend(
        validate_referential_integrity(
            gold
        )
    )

    results.extend(
        validate_revenue(
            gold
        )
    )

    # ========================================================
    # Final Result
    # ========================================================

    failed = sum(
        1
        for result in results
        if not result
    )

    passed = sum(
        1
        for result in results
        if result
    )

    print("\n" + "=" * 60)
    print("VALIDATION SUMMARY")
    print("=" * 60)

    print(f"Passed checks : {passed}")
    print(f"Failed checks : {failed}")
    print(f"Total checks  : {len(results)}")

    if failed == 0:
        print("=" * 60)
        print("GOLD VALIDATION PASSED")
        print("=" * 60)
    else:
        print("=" * 60)
        print("GOLD VALIDATION FAILED")
        print("=" * 60)

    spark.stop()

    if failed > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()