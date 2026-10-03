from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("RealtimeGoldToPostgreSQL")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Real-time PostgreSQL loader started...")

# Read new Gold Parquet files as a stream
gold_df = (
    spark.readStream
    .format("parquet")
    .schema(
        """
        window STRUCT<
            start: TIMESTAMP,
            end: TIMESTAMP
        >,
        category STRING,
        total_revenue DOUBLE,
        total_units_sold LONG,
        total_purchases LONG
        """
    )
    .load("data/gold/realtime_metrics")
)


def write_to_postgres(batch_df, batch_id):

    if batch_df.isEmpty():
        return

    print(f"Processing Gold batch: {batch_id}")

    rows = batch_df.collect()

    import psycopg2

    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="ecommerce_db",
        user="ecommerce_user",
        password="ecommerce_pass"
    )

    cursor = conn.cursor()

    for row in rows:

        cursor.execute(
            """
            INSERT INTO realtime_metrics
            (
                window_start,
                window_end,
                category,
                total_revenue,
                total_units_sold,
                total_purchases
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                row["window"]["start"],
                row["window"]["end"],
                row["category"],
                float(row["total_revenue"]),
                int(row["total_units_sold"]),
                int(row["total_purchases"])
            )
        )

    conn.commit()

    cursor.close()
    conn.close()

    print(f"Batch {batch_id} loaded into PostgreSQL.")


query = (
    gold_df.writeStream
    .foreachBatch(write_to_postgres)
    .outputMode("append")
    .option(
        "checkpointLocation",
        "data/gold_postgres_checkpoint"
    )
    .start()
)

query.awaitTermination()