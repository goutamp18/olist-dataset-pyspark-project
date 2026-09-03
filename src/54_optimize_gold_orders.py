from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, round, sum


spark = (
    SparkSession.builder
    .appName("OptimizeGoldOrders")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# 1. Read only the required Gold data
# --------------------------------------------------

orders = spark.read.parquet("data/gold/orders")

print("\nOriginal partitions:", orders.rdd.getNumPartitions())


# --------------------------------------------------
# 2. Filter early + select only required columns
# --------------------------------------------------

delivered_orders = (
    orders
    .filter(col("order_status") == "delivered")
    .select(
        "customer_unique_id",
        "order_id",
        "total_order_value"
    )
)

print("\nDelivered orders:", delivered_orders.count())

print("\n========== FILTER + COLUMN PRUNING ==========")
delivered_orders.explain("formatted")


# --------------------------------------------------
# 3. Aggregate
# --------------------------------------------------

customer_revenue = (
    delivered_orders
    .groupBy("customer_unique_id")
    .agg(
        count("order_id").alias("total_orders"),
        round(
            sum("total_order_value"), 2
        ).alias("total_spend")
    )
)

print("\nCustomer records:", customer_revenue.count())

print("\n========== AGGREGATION PLAN ==========")
customer_revenue.explain("formatted")


# --------------------------------------------------
# 4. Check partitions after aggregation
# --------------------------------------------------

print(
    "\nPartitions after aggregation:",
    customer_revenue.rdd.getNumPartitions()
)


# --------------------------------------------------
# 5. Repartition for final output
# --------------------------------------------------

optimized_output = customer_revenue.repartition(4)

print(
    "Partitions before write:",
    optimized_output.rdd.getNumPartitions()
)


# --------------------------------------------------
# 6. Write optimized Gold output
# --------------------------------------------------

output_path = "data/gold/customer_revenue_optimized"

(
    optimized_output
    .write
    .mode("overwrite")
    .parquet(output_path)
)

print("\nOptimized output written to:")
print(output_path)


# --------------------------------------------------
# 7. Final validation
# --------------------------------------------------

final_df = spark.read.parquet(output_path)

print("\n========== FINAL VALIDATION ==========")
print("Rows:", final_df.count())
print("Columns:", final_df.columns)
print("Partitions:", final_df.rdd.getNumPartitions())


spark.stop()