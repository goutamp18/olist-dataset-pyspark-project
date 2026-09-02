from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("OlistEcommercePipeline")
    .master("local[*]")
    .getOrCreate()
)

print("Spark version:", spark.version)


orders = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/raw/olist_orders_dataset.csv")
)


print("\n========== FIRST 10 ROWS ==========")
orders.show(10, truncate=False)


print("\n========== SCHEMA ==========")
orders.printSchema()


print("\n========== ROW COUNT ==========")
print("Rows:", orders.count())


spark.stop()