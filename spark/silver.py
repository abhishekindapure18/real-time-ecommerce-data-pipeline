from data_quality import apply_data_quality_checks
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType
)

spark = (
    SparkSession.builder
    .appName("EcommerceSilverStreaming")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Silver Streaming started...")
print("Spark version:", spark.version)


# Schema of incoming Kafka events
event_schema = StructType([
    StructField("event_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("event_type", StringType(), True),
    StructField("timestamp", StringType(), True)
])


# Read events from Kafka
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "ecommerce-events")
    .option("startingOffsets", "earliest")
    .load()
)


# Convert Kafka value from binary to string
json_df = kafka_df.selectExpr(
    "CAST(value AS STRING) AS json"
)


# Convert JSON string into columns
parsed_df = json_df.select(
    from_json(col("json"), event_schema).alias("data")
).select("data.*")


# Clean and transform the data
silver_df = (
    parsed_df
    .withColumn("timestamp", to_timestamp(col("timestamp")))
    .withColumn(
        "total_amount",
        col("price") * col("quantity")
    )
    .withColumn(
        "event_date",
        col("timestamp").cast("date")
    )
)

# Apply data quality rules
silver_df = apply_data_quality_checks(silver_df)


# Write cleaned data to Silver layer
query = (
    silver_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", "data/silver")
    .option("checkpointLocation", "data/silver_checkpoint")
    .partitionBy("event_date")
    .start()
)

query.awaitTermination()