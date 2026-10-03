from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, desc

spark = (
    SparkSession.builder
    .appName("EcommerceGoldAnalytics")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Gold Analytics started...")
print("Spark version:", spark.version)

# Read Silver data
silver_df = spark.read.parquet("data/silver")

print("Silver data loaded successfully.")

# -----------------------------
# 1. Revenue by Category
# -----------------------------

revenue_by_category = (
    silver_df
    .filter(silver_df.event_type == "purchase")
    .groupBy("category")
    .agg(
        sum("total_amount").alias("total_revenue"),
        count("*").alias("purchase_count")
    )
    .orderBy(desc("total_revenue"))
)

revenue_by_category.show()

revenue_by_category.write.mode("overwrite").parquet(
    "data/gold/revenue_by_category"
)

# -----------------------------
# 2. Top Selling Products
# -----------------------------

top_products = (
    silver_df
    .filter(silver_df.event_type == "purchase")
    .groupBy("product_id", "product_name")
    .agg(
        sum("quantity").alias("units_sold"),
        sum("total_amount").alias("revenue")
    )
    .orderBy(desc("units_sold"))
)

top_products.show()

top_products.write.mode("overwrite").parquet(
    "data/gold/top_products"
)

# -----------------------------
# 3. Overall Business Metrics
# -----------------------------

business_metrics = (
    silver_df
    .filter(silver_df.event_type == "purchase")
    .agg(
        sum("total_amount").alias("total_revenue"),
        sum("quantity").alias("total_units_sold"),
        count("*").alias("total_purchases")
    )
)

business_metrics.show()

business_metrics.write.mode("overwrite").parquet(
    "data/gold/business_metrics"
)

print("Gold analytics completed successfully.")

spark.stop()