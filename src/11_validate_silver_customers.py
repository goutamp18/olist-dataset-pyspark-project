from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("ValidateSilverCustomers")
    .master("local[*]")
    .getOrCreate()
)


SILVER_PATH = "data/silver/customers"

customers = spark.read.parquet(SILVER_PATH)


print("Silver Customers Count:", customers.count())

print("\nSilver Customers Schema:")
customers.printSchema()

print("\nSilver Customers Sample:")
customers.show(10, truncate=False)


spark.stop()