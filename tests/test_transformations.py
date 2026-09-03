import pytest
from pyspark.sql import SparkSession

from src.testing_utils import (
    calculate_order_value,
    validate_order_values,
    process_orders,
)

@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("PySparkTests")
        .getOrCreate()
    )

    yield spark

    spark.stop()


def test_calculate_order_value(spark):

    data = [
        ("order_1", 100.00, 10.00),
        ("order_2", 200.00, 20.00),
        ("order_3", 50.50, 5.25),
    ]

    columns = [
        "order_id",
        "product_revenue",
        "freight_revenue",
    ]

    df = spark.createDataFrame(data, columns)

    result = calculate_order_value(df)

    actual = [
        row.total_order_value
        for row in result.orderBy("order_id").collect()
    ]

    expected = [
        110.00,
        220.00,
        55.75,
    ]

    assert actual == expected

def test_calculate_order_value_zero_freight(spark):

    data = [
        ("order_1", 100.00, 0.00),
        ("order_2", 250.50, 0.00),
    ]

    columns = [
        "order_id",
        "product_revenue",
        "freight_revenue",
    ]

    df = spark.createDataFrame(data, columns)

    result = calculate_order_value(df)

    actual = [
        row.total_order_value
        for row in result.orderBy("order_id").collect()
    ]

    expected = [
        100.00,
        250.50,
    ]

    assert actual == expected


def test_calculate_order_value_rounding(spark):

    data = [
        ("order_1", 100.123, 10.456),
    ]

    columns = [
        "order_id",
        "product_revenue",
        "freight_revenue",
    ]

    df = spark.createDataFrame(data, columns)

    result = calculate_order_value(df)

    actual = result.first().total_order_value

    expected = 110.58

    assert actual == expected

def test_calculate_order_value_with_null(spark):

    data = [
        ("order_1", None, 10.00),
        ("order_2", 100.00, None),
        ("order_3", None, None),
    ]

    columns = [
        "order_id",
        "product_revenue",
        "freight_revenue",
    ]

    df = spark.createDataFrame(data, columns)

    result = calculate_order_value(df)

    actual = [
        row.total_order_value
        for row in result.orderBy("order_id").collect()
    ]

    expected = [
        None,
        None,
        None,
    ]

    assert actual == expected

def test_validate_order_values(spark):

    data = [
        ("order_1", 100.00),
        ("order_2", -50.00),
        ("order_3", 200.00),
        ("order_4", -10.00),
    ]

    columns = [
        "order_id",
        "total_order_value",
    ]

    df = spark.createDataFrame(data, columns)

    invalid_records = validate_order_values(df)

    actual = [
        row.order_id
        for row in invalid_records.orderBy("order_id").collect()
    ]

    expected = [
        "order_2",
        "order_4",
    ]

    assert actual == expected

def test_calculate_order_value_schema(spark):

    data = [
        ("order_1", 100.00, 10.00),
    ]

    columns = [
        "order_id",
        "product_revenue",
        "freight_revenue",
    ]

    df = spark.createDataFrame(data, columns)

    result = calculate_order_value(df)

    expected_schema = {
        "order_id": "string",
        "product_revenue": "double",
        "freight_revenue": "double",
        "total_order_value": "double",
    }

    actual_schema = {
        field.name: field.dataType.simpleString()
        for field in result.schema.fields
    }

    assert actual_schema == expected_schema

def test_process_orders_pipeline(spark):

    data = [
        ("order_1", 100.00, 10.00),
        ("order_2", 200.00, 20.00),
        ("order_3", -50.00, 10.00),
    ]

    columns = [
        "order_id",
        "product_revenue",
        "freight_revenue",
    ]

    df = spark.createDataFrame(data, columns)

    result = process_orders(df)

    actual = [
        (row.order_id, row.total_order_value)
        for row in result.orderBy("order_id").collect()
    ]

    expected = [
        ("order_1", 110.00),
        ("order_2", 220.00),
    ]

    assert actual == expected