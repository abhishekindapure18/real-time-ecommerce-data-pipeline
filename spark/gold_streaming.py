from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    window,
    sum,
    count
)

spark = (
    SparkSession.builder
    .appName("EcommerceGoldStreaming")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Gold Streaming started...")
print("Spark version:", spark.version)

# Read Silver data as a streaming source
silver_df = (
    spark.readStream
    .format("parquet")
    .schema(
        """
        event_id INT,
        user_id INT,
        product_id INT,
        product_name STRING,
        category STRING,
        price DOUBLE,
        quantity INT,
        event_type STRING,
        timestamp TIMESTAMP,
        total_amount DOUBLE,
        event_date DATE
        """
    )
    .load("data/silver")
)

# Keep only purchase events
purchase_df = silver_df.filter(
    col("event_type") == "purchase"
)

# Create 1-minute windowed business metrics
gold_df = (
    purchase_df
    .withWatermark("timestamp", "2 minutes")
    .groupBy(
        window(col("timestamp"), "1 minute"),
        col("category")
    )
    .agg(
        sum("total_amount").alias("total_revenue"),
        sum("quantity").alias("total_units_sold"),
        count("*").alias("total_purchases")
    )
)

# Write Gold analytics to Parquet
query = (
    gold_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", "data/gold/realtime_metrics")
    .option(
        "checkpointLocation",
        "data/gold_realtime_checkpoint"
    )
    .start()
)

query.awaitTermination()