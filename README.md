# Real-Time E-Commerce Data Engineering Pipeline

A real-time e-commerce data engineering pipeline built using **Apache Kafka, PySpark Structured Streaming, PostgreSQL, Docker, and Streamlit**.

The project simulates real-time customer activities such as product views, cart additions, and purchases. Events are streamed through Kafka, processed using PySpark, cleaned and transformed through Bronze, Silver, and Gold layers, stored in PostgreSQL, and visualized through an interactive dashboard.

## Architecture

```text
                    ┌─────────────────────┐
                    │  Python Event       │
                    │     Generator       │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │   Kafka Producer    │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │       Kafka         │
                    │ ecommerce-events    │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │      PySpark        │
                    │ Structured Streaming│
                    └──────────┬──────────┘
                               │
                 ┌─────────────┼─────────────┐
                 ▼             ▼             ▼
            ┌─────────┐  ┌──────────┐  ┌─────────┐
            │ Bronze  │  │  Silver  │  │  Gold   │
            │  Raw    │→ │ Cleaned  │→ │Analytics│
            └─────────┘  └──────────┘  └────┬────┘
                                             │
                              ┌──────────────┴──────────────┐
                              ▼                             ▼
                       ┌─────────────┐              ┌─────────────┐
                       │ PostgreSQL  │              │  Streamlit  │
                       │  Analytics  │              │  Dashboard  │
                       └─────────────┘              └─────────────┘
```

## Tech Stack

| Technology   | Purpose                                |
| ------------ | -------------------------------------- |
| Python       | Event generation and application logic |
| Apache Kafka | Real-time event streaming              |
| PySpark      | Stream processing and analytics        |
| PostgreSQL   | Analytical data storage                |
| SQL          | Business analytics                     |
| Docker       | Running Kafka and PostgreSQL           |
| Streamlit    | Data visualization dashboard           |
| Parquet      | Data storage format                    |

## Features

* Real-time e-commerce event generation
* Kafka-based event streaming
* PySpark Structured Streaming
* Bronze, Silver, and Gold data architecture
* Data quality validation
* Parquet-based data storage
* Date-partitioned Silver data
* Real-time window-based aggregations
* Revenue analytics
* Product-level analytics
* PostgreSQL integration
* SQL business reporting
* Interactive Streamlit dashboard
* Dockerized infrastructure

## Project Structure

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
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Data Pipeline

### 1. Event Generation

The Python event generator creates simulated e-commerce events containing:

* Event ID
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

### 2. Kafka

Events are published to the Kafka topic:

```text
ecommerce-events
```

Kafka acts as the message broker between the event producer and Spark streaming application.

### 3. Bronze Layer

PySpark consumes events from Kafka and stores the incoming event data in the Bronze layer.

```text
Kafka → Bronze
```

The Bronze layer contains the raw event information before further transformation.

### 4. Silver Layer

The Silver layer parses and transforms the incoming JSON events.

Data processing includes:

* JSON schema parsing
* Timestamp conversion
* Total amount calculation
* Event date extraction
* Null validation
* Event type validation
* Price validation
* Quantity validation

The following field is calculated:

```text
total_amount = price × quantity
```

Silver data is stored in Parquet format and partitioned by:

```text
event_date
```

### 5. Gold Layer

The Gold layer contains business-oriented analytics generated from the Silver data.

Current analytics include:

* Revenue by category
* Top products
* Total units sold
* Total purchases
* Total revenue

The project also implements real-time analytics using one-minute windows with category-level aggregations.

```text
Purchase Events
      ↓
1-Minute Window
      ↓
Category Aggregation
      ↓
Revenue / Units / Purchases
```

### 6. PostgreSQL

Processed Gold data is loaded into PostgreSQL for SQL-based analysis and dashboard consumption.

Main tables:

```text
revenue_by_category
top_products
realtime_metrics
ecommerce_metrics
```

### 7. Streamlit Dashboard

The Streamlit dashboard provides a visual interface for the processed data.

It includes:

* Total revenue
* Total units sold
* Total purchases
* Number of categories
* Revenue by category
* Revenue trends
* Top products
* Category summary
* Recent streaming windows

## Getting Started

### Prerequisites

Install the following:

* Python 3.x
* Java
* Apache Spark
* Docker Desktop
* Git

Make sure Docker Desktop is running before starting Kafka and PostgreSQL.

### 1. Clone the Repository

```bash
git clone https://github.com/abhishekindapure18/real-time-ecommerce-data-pipeline.git
cd real-time-ecommerce-data-pipeline
```

### 2. Create a Virtual Environment

Windows:

```powershell
python -m venv .venv
```

Activate it:

```powershell
.venv\Scripts\activate
```

### 3. Install Python Dependencies

```powershell
pip install -r requirements.txt
```

### 4. Start Docker Services

```powershell
docker compose up -d
```

Check running containers:

```powershell
docker ps
```

The following services should be running:

```text
ecommerce-kafka
ecommerce-postgres
```

## Running the Pipeline

### Step 1 — Start the Event Generator

```powershell
python data-generator/generate_events.py
```

This generates simulated e-commerce events.

### Step 2 — Start the Kafka Producer

```powershell
python kafka/producer.py
```

The producer publishes events to:

```text
ecommerce-events
```

### Step 3 — Run the Silver Streaming Pipeline

```powershell
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 spark/silver.py
```

This consumes Kafka events, validates and transforms them, and writes the Silver layer.

### Step 4 — Generate Gold Analytics

```powershell
spark-submit spark/gold.py
```

This generates:

```text
data/gold/revenue_by_category
data/gold/top_products
data/gold/business_metrics
```

### Step 5 — Load Gold Data into PostgreSQL

```powershell
python sql/load_to_postgres.py
```

### Step 6 — Start the Dashboard

```powershell
streamlit run dashboard/app.py
```

The Streamlit dashboard will open in your browser.

## PostgreSQL Configuration

The local PostgreSQL database is configured through Docker Compose.

```text
Host: localhost
Port: 5432
Database: ecommerce_db
User: ecommerce_user
Password: ecommerce_pass
```

The database schema is defined in:

```text
sql/schema.sql
```

## SQL Analytics

The project contains SQL queries for:

* Revenue by category
* Top products by units sold
* Top products by revenue
* Total units sold
* Total revenue
* Number of products

SQL queries are available in:

```text
sql/analytics.sql
```

## Data Engineering Concepts

This project demonstrates practical data engineering concepts including:

* Real-time data ingestion
* Event-driven architecture
* Message queues
* Kafka producers and consumers
* Structured Streaming
* ETL processing
* Data validation
* Data quality checks
* Medallion architecture
* Parquet storage
* Data partitioning
* Window-based aggregations
* Batch analytics
* Streaming analytics
* PostgreSQL integration
* SQL analytics
* Docker-based infrastructure
* Dashboard development

## Data Quality

The Silver layer applies basic validation rules to incoming events.

Examples include:

```text
event_id must not be NULL
user_id must not be NULL
product_id must not be NULL
price must be greater than 0
quantity must be greater than 0
event_type must be view, cart, or purchase
```

Invalid records are filtered before the data reaches the analytics layer.

## Future Improvements

Potential improvements include:

* Cloud deployment using Azure or AWS
* Cloud data lake integration
* Apache Airflow orchestration
* Automated data quality monitoring
* Schema evolution
* Kafka consumer scaling
* Improved PostgreSQL batch loading
* Automated testing
* CI/CD pipeline
* Authentication and secure environment variables
* Production-scale deployment using Docker

## Author

**Abhishek Indapure**

GitHub: `https://github.com/abhishekindapure18`
