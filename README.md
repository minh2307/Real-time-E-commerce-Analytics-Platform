# Real-time E-commerce Analytics Platform

This repository contains a full-stack, end-to-end Real-time E-commerce Analytics Platform. It is designed with a microservices-based backend, an event-driven architecture using Kafka, and a modern data pipeline built with Apache Spark and DuckDB. 

## 🏗 Architecture

The platform is divided into two major components:

### 1. Backend Systems (Spring Boot)
- **Domain-Driven Design (DDD):** Divided into `user`, `catalog`, `inventory`, `cart`, `order`, and `payment` modules.
- **Outbox Pattern:** Ensures reliable event delivery. Domain events (e.g., `ORDER_CREATED`, `PAYMENT_COMPLETED`) are safely stored in Postgres alongside business transactions.
- **Microservices-ready:** Managed boundaries enforced via **ArchUnit** tests, preventing tight coupling between domains.
- **Kafka Connect / Poller:** Outbox events are pushed to Kafka (`ecommerce.events` topic).

### 2. Data Pipeline (Apache Spark & DuckDB)
- **Bronze Layer (Ingestion):** Spark Streaming consumer reads raw JSON from Kafka and writes raw Parquet files to the Bronze data lake.
- **Silver Layer (Cleansing):** Deduplicates events, extracts fields, and validates schemas. Drops malformed records to a quarantine zone.
- **Gold Layer (Data Warehouse):** Dimensional modeling (Star Schema). 
  - **Dimensions:** `dim_customers`, `dim_products`, `dim_categories`
  - **Facts:** `fact_orders`, `fact_order_items`, `fact_payments`, `fact_product_views`
- **Analytics & Validation:** DuckDB is used to run high-performance SQL analytics directly on the Gold Parquet files. An automated data quality gate ensures no GMV duplication or fact multiplication.

## 🚀 Tech Stack

- **Backend:** Java 21, Spring Boot 3.2, PostgreSQL 16, Gradle
- **Streaming:** Apache Kafka, Kafka-Python
- **Data Engineering:** Apache Spark (PySpark) 3.5.0, DuckDB, PyArrow, Pandas
- **Testing:** Pytest, JUnit 5, ArchUnit, Testcontainers

## ⚙️ How to Run Locally

### Prerequisites
- Docker & Docker Compose
- Java 21+
- Python 3.12+

### 1. Start Infrastructure
Start PostgreSQL, Kafka, and Zookeeper:
```bash
docker-compose up -d
```

### 2. Start the Backend API
```bash
cd backend
./gradlew bootRun
```
*The API will be available at `http://localhost:8080`.*

### 3. Generate App Traffic
Simulate a full end-to-end user journey (register, login, create category/product, restock, add to cart, checkout, pay):
```bash
export PYTHONPATH=$(pwd)/data-pipeline
./data-pipeline/.venv-de/bin/python test/e2e/simulate_app_traffic.py
```

### 4. Run the Data Pipeline
Execute the Medallion Architecture pipeline to process the events:
```bash
export DATA_DIR=$(pwd)/data-pipeline/data
export PYTHONPATH=$(pwd)/data-pipeline

# 1. Bronze (Stop manually after it consumes messages, e.g., using timeout)
timeout 10 ./data-pipeline/.venv-de/bin/python -m bronze.main

# 2. Silver
./data-pipeline/.venv-de/bin/python -m silver.main

# 3. Gold
./data-pipeline/.venv-de/bin/python -m gold.main
```

### 5. Validate Analytics Data Quality
Run the DuckDB data quality validation script to verify GMV, facts, and dimensional integrity:
```bash
./data-pipeline/.venv-de/bin/python data-pipeline/validate_analytics.py
```

## ✅ Test Results & Quality Gates

The project has undergone rigorous testing and validation:
- **Architecture Validation:** 100% pass on ArchUnit constraints (ensuring strict domain boundaries).
- **Unit & Integration Tests:** Comprehensive JUnit tests for backend domains.
- **Data Quality:** Validation script strictly asserts mathematical constraints (e.g., `SUM(payments) == GMV` for completed transactions) to prevent data duplication issues common in naive JOINs.
- **End-to-End Idempotency:** The Spark Silver layer deduplicates messages with exactly-once guarantees by leveraging `event_id`.

## ⚠️ Known Limitations (MVP Scope)
- **Airflow Orchestration:** Currently, pipelines are triggered sequentially via shell scripts. Integrating Apache Airflow for scheduling and dependency management is the next planned step.
- **CI/CD:** GitHub Actions for automated testing and linting are not yet implemented.
- **Metrics/Observability:** Lacks Prometheus/Grafana integration for monitoring application health and pipeline latency.

---
*Developed as a Portfolio Project demonstrating best practices in Software Engineering and Data Engineering.*
