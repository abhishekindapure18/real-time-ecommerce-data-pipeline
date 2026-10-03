# Real-Time E-Commerce Data Engineering Pipeline

A real-time data engineering pipeline that ingests e-commerce events through **Apache Kafka**, processes them using **PySpark Structured Streaming**, stores analytics in **PostgreSQL**, and visualizes business metrics through a **Streamlit dashboard**.

## 🚀 Project Overview

E-commerce platforms generate a continuous stream of customer events such as product views, cart additions, and purchases.

This project demonstrates how to build an end-to-end pipeline that can:

* Ingest real-time customer events
* Process and transform streaming data
* Apply data quality checks
* Organize data using a **Bronze–Silver–Gold architecture**
* Generate real-time business metrics
* Store analytical results in PostgreSQL
* Visualize sales and revenue through a dashboard

## 🏗️ Architecture

```text
                    ┌─────────────────────┐
                    │  Python Event       │
                    │     Generator       │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │   Apache Kafka      │
                    │  ecommerce-events   │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │ PySpark Structured  │
                    │     Streaming       │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │      Bronze         │
                    │    Raw Events       │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │      Silver         │
                    │ Cleaned + Validated │
                    │      Parquet        │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │       Gold          │
                    │ Business Analytics  │
                    └───────┬───────┬─────┘
                            │       │
                            ▼       ▼
                   ┌────────────┐ ┌─────────────┐
                   │ PostgreSQL │ │  Streamlit  │
                   │ Analytics  │ │  Dashboard  │
                   └────────────┘ └─────────────┘
```

## 🛠️ Tech Stack

| Category             | Technology                 |
| -------------------- | -------------------------- |
| Programming          | Python                     |
| Streaming            | Apache Kafka               |
| Processing           | Apache Spark / PySpark     |
| Streaming Processing | Spark Structured Streaming |
| Storage              | Parquet                    |
| Database             | PostgreSQL                 |
| Dashboard            | Streamlit                  |
| Containers           | Docker                     |
| Query Language       | SQL                        |
| Version Control      | Git / GitHub               |

## 📂 Project Structure

```text
real-time-ecommerce-data-pipeline/
│
├── data-generator/
│   └── generate_events.py
│
├── kafka/
│   ├── producer.py
│   └── consumer.py
│
├── spark/
│   ├── streaming.py
│   ├── silver.py
│   ├── gold.py
│   ├── gold_streaming.py
│   └── data_quality.py
│
├── sql/
│   ├── schema.sql
│   ├── analytics.sql
│   ├── load_to_postgres.py
│   └── load_realtime_to_postgres.py
│
├── dashboard/
│   └── app.py
│
├── data/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── requirements.txt
├── docker-compose.yml
├── .gitignore
└── README.md
```

## 🔄 Data Flow

### 1. Event Generation

Python generates simulated e-commerce events containing:

* User ID
* Product ID
* Product name
* Category
* Price
* Quantity
* Event type
* Timestamp

Supported event types:

```text
view
cart
purchase
```

### 2. Kafka Ingestion

Events are published to the Kafka topic:

```text
ecommerce-events
```

Kafka provides the streaming ingestion layer between the event producer and Spark processing.

### 3. Bronze Layer

The Bronze layer stores the incoming raw event stream.

```text
Kafka → Bronze
```

This preserves the original event payload before transformation.

### 4. Silver Layer

PySpark parses the JSON events and converts them into structured records.

The Silver layer performs:

* JSON parsing
* Schema enforcement
* Timestamp conversion
* Total amount calculation
* Event-date extraction
* Data quality validation
* Parquet storage
* Date-based partitioning

Example calculated field:

```text
total_amount = price × quantity
```

### 5. Gold Layer

The Gold layer converts cleaned Silver data into business-level analytics.

Current analytics include:

* Revenue by category
* Top products by units sold
* Top products by revenue
* Total revenue
* Total units sold
* Total purchases

### 6. Real-Time Analytics

Spark Structured Streaming performs window-based aggregations on purchase events.

Example:

```text
1-minute window
        ↓
Category
        ↓
Revenue
Units Sold
Purchase Count
```

These real-time metrics are written to Parquet and streamed into PostgreSQL.

### 7. PostgreSQL

PostgreSQL stores the analytical results for querying and dashboard consumption.

Main tables:

```text
revenue_by_category
top_products
ecommerce_metrics
realtime_metrics
```

### 8. Dashboard

A Streamlit dashboard provides a visual view of the processed data.

The dashboard includes:

* Total revenue
* Total units sold
* Purchase metrics
* Revenue by category
* Revenue trends
* Top products
* Recent streaming metrics
* Category-level summaries

## 🧪 Data Quality

The Silver layer validates incoming events using rules such as:

```text
event_id must not be NULL
user_id must not be NULL
product_id must not be NULL
price must be greater than 0
quantity must be greater than 0
event_type must be view, cart, or purchase
```

Invalid records are filtered before downstream analytics.

## 📊 Key Engineering Concepts Demonstrated

This project provides hands-on exposure to:

* Real-time data ingestion
* Event-driven architecture
* Kafka producers and consumers
* Spark Structured Streaming
* Micro-batch processing
* Bronze–Silver–Gold architecture
* Data quality validation
* Parquet data storage
* Partitioning
* Window-based aggregations
* Streaming checkpoints
* PostgreSQL integration
* SQL analytics
* Docker-based services
* Dashboard development
* Git/GitHub version control

## ⚙️ Setup

### Prerequisites

Install:

* Python 3.x
* Java
* Apache Spark
* Docker Desktop
* PostgreSQL client tools
* Git

For Windows Spark execution, configure Hadoop/WinUtils if required by the local environment.

### 1. Clone the Repository

```bash
git clone https://github.com/abhishekindapure18/real-time-ecommerce-data-pipeline.git

cd real-time-ecommerce-data-pipeline
```

### 2. Create Virtual Environment

```bash
python -m venv venv
```

Activate it on Windows:

```powershell
venv\Scripts\activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Start Kafka and PostgreSQL

```bash
docker compose up -d
```

Verify the containers:

```bash
docker ps
```

### 5. Create Kafka Topic

```bash
docker exec -it ecommerce-kafka /opt/kafka/bin/kafka-topics.sh --create --topic ecommerce-events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

### 6. Start the Event Producer

```bash
python kafka/producer.py
```

### 7. Start Spark Streaming

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 spark/streaming.py
```

### 8. Process Silver Data

```bash
spark-submit spark/silver.py
```

### 9. Generate Gold Analytics

```bash
spark-submit spark/gold.py
```

### 10. Load Gold Analytics into PostgreSQL

```bash
spark-submit sql/load_to_postgres.py
```

### 11. Start Real-Time Gold Processing

```bash
spark-submit spark/gold_streaming.py
```

### 12. Load Real-Time Metrics into PostgreSQL

```bash
spark-submit sql/load_realtime_to_postgres.py
```

### 13. Start Dashboard

```bash
streamlit run dashboard/app.py
```

## 🗄️ PostgreSQL Configuration

The local Docker PostgreSQL configuration uses:

```text
Database: ecommerce_db
Username: ecommerce_user
Password: ecommerce_pass
Port: 5432
```

For production deployments, credentials should be moved to environment variables or a secrets manager.

## 📈 Example SQL Analytics

### Revenue by Category

```sql
SELECT
    category,
    ROUND(total_revenue::numeric, 2) AS revenue,
    purchase_count
FROM revenue_by_category
ORDER BY revenue DESC;
```

### Top Products

```sql
SELECT
    product_id,
    product_name,
    units_sold,
    revenue
FROM top_products
ORDER BY revenue DESC
LIMIT 5;
```

### Real-Time Metrics

```sql
SELECT
    category,
    total_revenue,
    total_units_sold,
    total_purchases
FROM realtime_metrics
ORDER BY created_at DESC;
```

## 🎯 Outcome

The project demonstrates a working end-to-end data pipeline capable of:

```text
Real-Time Events
      ↓
Kafka Ingestion
      ↓
Spark Streaming
      ↓
Data Quality
      ↓
Bronze / Silver / Gold
      ↓
Real-Time Aggregations
      ↓
PostgreSQL
      ↓
Dashboard
```

During validation, real-time streaming batches were successfully processed and **420 real-time metric records were loaded into PostgreSQL**.

## 🔮 Future Improvements

Potential extensions include:

* Deploying the pipeline on Azure or AWS
* Using Azure Data Lake Storage
* Adding Databricks
* Implementing automated data-quality reporting
* Adding schema evolution
* Improving PostgreSQL batch-write performance
* Adding monitoring and alerting
* Containerizing the complete pipeline
* Adding CI/CD with GitHub Actions
* Adding automated tests
* Scaling Kafka across multiple brokers

## 👨‍💻 Author

**Abhishek Indapure**


GitHub: `https://github.com/abhishekindapure18`

---

⭐ If you found this project useful, consider giving the repository a star.
