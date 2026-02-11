# End-to-End Data Engineering Pipeline for E-Commerce Orders

An end-to-end data engineering project that simulates a production-grade pipeline for ingesting, transforming, and modeling e-commerce data into a structured Data Warehouse.

This project demonstrates modern data engineering concepts including incremental loading, idempotent pipelines, bulk operations, staging layers, dimensional modeling, and performance optimization.

---

## 📌 Project Overview

This pipeline processes raw e-commerce CSV data and builds a structured analytical Data Warehouse using a layered architecture:

Raw Layer → Clean Layer → Staging → Dimensional Model (Star Schema)

Technologies used:
- Python
- Pandas
- MongoDB
- PostgreSQL
- Docker
- SQL

---

## 🗂 Data Source

Dataset: Brazilian E-Commerce Public Dataset by Olist

Source:
https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

Files used:
- orders.csv
- order_items.csv
- customers.csv
- products.csv

The dataset contains real-world e-commerce transactional data including:
- Orders
- Order items
- Customers
- Products
- Timestamps
- Pricing data

---

## 🏗 Architecture

The project follows a layered architecture:

### 1️⃣ Ingestion Layer
- Reads raw CSV files using Pandas
- Logs ingestion steps
- Validates file loading

### 2️⃣ Raw Layer (MongoDB)
- Raw data stored incrementally
- Deterministic _id generation
- Bulk upsert operations
- Idempotent design

### 3️⃣ Transformation Layer
- Clean column normalization
- Merge logic:
  - orders + order_items
  - + customers
  - + products
- Fact table built at Order Item grain level

### 4️⃣ Staging Layer (PostgreSQL)
- Dynamic staging table creation
- COPY for high-performance loading
- Schema validation before load

### 5️⃣ Data Warehouse Layer
Star Schema:

- dim_customers
- dim_products
- fact_orders

Primary key:
(order_id, order_item_id)

Foreign keys:
- customer_id → dim_customers
- product_id → dim_products

---

## ⭐ Key Features

### ✅ Incremental Loading
- MongoDB uses bulk upsert
- PostgreSQL uses INSERT ... ON CONFLICT DO NOTHING
- Idempotent pipeline execution

### ✅ Performance Optimization
- Bulk Mongo writes
- PostgreSQL COPY for fast ingestion
- Chunked batch processing
- Index usage on primary keys

### ✅ Deterministic Primary Keys
MongoDB _id is generated based on business keys:
- order_id
- order_item_id
- customer_id
- product_id

### ✅ Grain Definition
Fact table defined at:
Order Item Level

This ensures:
- No duplication
- Accurate dimensional modeling
- Referential integrity

### ✅ Logging
- Structured logging across modules
- Progress tracking for bulk operations
- Error logging with stack traces

### ✅ Clean Architecture
Separation of concerns:
- ingestion/
- transformation/
- infrastructure/
- core/

Repository pattern for database interaction.

---

## 🔄 Incremental Strategy

### MongoDB
Bulk upsert using UpdateOne with upsert=True.
Ensures:
- No duplication
- Efficient re-runs

### Data Warehouse
Uses:
INSERT ... ON CONFLICT DO NOTHING

This guarantees idempotency and safe re-execution.

---

## 🧠 Data Modeling

### Fact Table Grain
Each row represents:
One Order Item

Primary Key:
(order_id, order_item_id)

Why?
Because a single order may contain the same product multiple times.

This modeling decision prevents incorrect deduplication.

---

## 🚀 How to Run

### 1️⃣ Setup Environment

```bash
pip install -r requirements.txt
```

Create a `.env` file:

```
MONGO_URI=your_mongo_connection_string
DB_NAME=ecommerce_db
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=your_db
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_password
```

---

### 2️⃣ Run the Pipeline

```bash
python src/main.py
```

The pipeline will:

1. Load raw CSV files
2. Upsert into MongoDB
3. Build fact table
4. Load staging table
5. Populate Data Warehouse tables incrementally

---

## 📊 Performance Considerations

- COPY is used for fast CSV ingestion
- Bulk operations reduce MongoDB round-trips
- Indexes accelerate conflict resolution
- Chunking prevents memory overload

Designed to scale to hundreds of thousands of records efficiently.

---

## 📈 Future Improvements

- Slowly Changing Dimensions (SCD Type 2)
- Airflow orchestration
- Data Quality validation framework
- Unit tests for transformations
- CI/CD integration
- Partitioned fact tables
- Query performance benchmarking

---

## 📚 Concepts Demonstrated

- ETL vs ELT
- Incremental data loading
- Idempotent pipelines
- Dimensional modeling
- Star schema design
- Grain definition
- Conflict handling
- Bulk database operations
- Production-style logging

---

## 🎯 Project Goal

This project was built to simulate real-world data engineering workflows and demonstrate:

- End-to-end pipeline thinking
- Strong data modeling foundations
- Performance awareness
- Clean code architecture
- Debugging and problem-solving skills

---

## 👨‍💻 Author

Ahmed Salem  
Data Engineering Track

---

