import psycopg2
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("GoldToPostgreSQL")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="ecommerce_db",
    user="ecommerce_user",
    password="ecommerce_pass"
)

cursor = conn.cursor()

print("Connected to PostgreSQL.")

revenue_df = spark.read.parquet(
    "data/gold/revenue_by_category"
)

revenue_data = revenue_df.collect()

for row in revenue_data:
    cursor.execute(
        """
        INSERT INTO revenue_by_category
        (category, total_revenue, purchase_count)
        VALUES (%s, %s, %s)
        """,
        (
            row["category"],
            float(row["total_revenue"]),
            int(row["purchase_count"])
        )
    )

products_df = spark.read.parquet(
    "data/gold/top_products"
)

products_data = products_df.collect()

for row in products_data:
    cursor.execute(
        """
        INSERT INTO top_products
        (product_id, product_name, units_sold, revenue)
        VALUES (%s, %s, %s, %s)
        """,
        (
            int(row["product_id"]),
            row["product_name"],
            int(row["units_sold"]),
            float(row["revenue"])
        )
    )

conn.commit()

print("Gold data successfully loaded into PostgreSQL.")

cursor.close()
conn.close()
spark.stop()

print("PostgreSQL connection closed.")