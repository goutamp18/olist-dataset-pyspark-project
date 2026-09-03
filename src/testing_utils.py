
from pyspark.sql import functions as F


def calculate_order_value(df):
    return df.withColumn(
        "total_order_value",
        F.round(
            F.col("product_revenue") + F.col("freight_revenue"),
            2
        )
    )

def validate_order_values(df):
    """
    Return records where total_order_value is negative.
    """

    return df.filter(
        F.col("total_order_value") < 0
    )

def process_orders(df):
    """
    Apply order-value calculation and return
    only valid order records.
    """

    result = calculate_order_value(df)

    valid_orders = result.filter(
        F.col("total_order_value") >= 0
    )

    return valid_orders