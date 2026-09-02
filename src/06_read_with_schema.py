from pyspark.sql import SparkSession
from schemas.olist_schemas import orders_schema


spark = (
    SparkSession.builder
    .appName("OlistExplicitSchema")
    .master("local[*]")
    .getOrCreate()
)


orders = (
    spark.read
    .option("header", True)
    .schema(orders_schema)
    .csv("data/raw/olist_orders_dataset.csv")
)


print("\n========== SCHEMA ==========")
orders.printSchema()

print("\n========== SAMPLE DATA ==========")
orders.show(5, truncate=False)

print("\n========== ROW COUNT ==========")
print("Rows:", orders.count())


spark.stop()