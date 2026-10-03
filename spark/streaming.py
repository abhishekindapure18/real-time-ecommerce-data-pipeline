from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("EcommerceKafkaStreaming")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Spark Streaming started...")
print("Spark version:", spark.version)

# Read events from Kafka
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "ecommerce-events")
    .option("startingOffsets", "earliest")
    .load()
)

# Kafka gives us key/value as binary.
# Convert the value to string.
events = df.selectExpr("CAST(value AS STRING) AS event")

# Write raw events to Bronze
query = (
    events.writeStream
    .format("text")
    .outputMode("append")
    .option("path", "data/bronze")
    .option("checkpointLocation", "data/checkpoint")
    .start()
)

query.awaitTermination()