# Real-time E-commerce Analytics Platform

This repository houses the **Real-time E-commerce Analytics Platform**, an end-to-end operational and analytical system designed for dual-tier processing. It features a transactional backend for managing the e-commerce lifecycle and a decoupled event-driven data pipeline for generating analytics and Gross Merchandise Value (GMV) reports.

## High-Level Architecture
- **Operational Zone:** A Spring Boot Modular Monolith executing strict transactional invariants on a PostgreSQL database. Uses the Transactional Outbox Pattern to asynchronously publish state changes.
- **Event Bus:** Apache Kafka buffers domain events emitted by the outbox poller.
- **Analytical Zone:** A Medallion Lakehouse (Bronze, Silver, Gold). Python ingestion daemons write raw data to Bronze. PySpark batch jobs cleanse data into Silver and build Kimball Star Schema dimensions and facts into Gold.
- **Analytical Store:** DuckDB enables fast columnar queries directly over Gold Parquet files.
- **Orchestration:** Apache Airflow manages data pipeline schedules and dependencies.

## Main Technology Areas
- **Backend:** Java 21, Spring Boot 3, Gradle, Flyway, PostgreSQL 16
- **Messaging:** Apache Kafka (Kraft Mode)
- **Data Engineering:** Python, PySpark (Local mode)
- **Orchestration:** Apache Airflow (LocalExecutor)
- **Analytics:** DuckDB (Embedded)
- **Infrastructure:** Docker Compose

## Source of Truth Documentation
The architectural guidelines and project specifications are meticulously detailed in the following documents:
- [01 Project Brief](docs/01-project-brief.md)
- [02 Requirements](docs/02-requirements.md)
- [03 Use Cases](docs/03-use-cases.md)
- [04 System Architecture](docs/04-system-architecture.md)
- [05 Database Design](docs/05-database-design.md)
- [06 Data Model](docs/06-data-model.md)
- [07 Data Pipeline](docs/07-data-pipeline.md)
- [08 API Specification](docs/08-api-specification.md)
- [09 Implementation Plan](docs/09-implementation-plan.md)

## Current Implementation Status
The project MVP is locally complete. The backend schema, transactional outbox, Kafka event bus, Bronze/Silver/Gold pipelines, DuckDB validation, and Airflow orchestration are fully implemented.

**Current Milestone:** `TASK 12 — Airflow Orchestration & Local E2E Validation`

## Local Development Infrastructure

The infrastructure is managed via Docker Compose profiles to respect local memory limits.

### Minimal Backend Profile (PostgreSQL + Redis)
Start the backend dependencies (PostgreSQL 16 and Redis 7):
```bash
docker compose -f infra/docker-compose.yml --profile backend up -d
```

Check the status and healthchecks:
```bash
docker compose -f infra/docker-compose.yml --profile backend ps
```

Stop the services:
```bash
docker compose -f infra/docker-compose.yml --profile backend down
```

### Events Profile (Kafka)
Start the Kafka broker in KRaft mode:
```bash
docker compose -f infra/docker-compose.yml --profile events up -d
```

### Data Engineering Profile (Airflow & Pipeline)
Airflow is set up natively in the Python `.venv-de` virtual environment for extreme resource efficiency. It operates on an embedded SQLite metadata database.

**Starting the Airflow UI & Scheduler locally:**
```bash
export AIRFLOW_HOME=$(pwd)/data-pipeline/airflow_home
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/data-pipeline/dags

# Initialize DB (run once)
data-pipeline/.venv-de/bin/airflow db migrate
data-pipeline/.venv-de/bin/airflow users create -u admin -p admin -f Admin -l User -r Admin -e admin@example.com

# Start the webserver (UI at http://localhost:8080)
data-pipeline/.venv-de/bin/airflow webserver --port 8080 &

# Start the scheduler
data-pipeline/.venv-de/bin/airflow scheduler &
```

**Triggering the E2E DAG:**
- DAG ID: `ecommerce_local_e2e`
- Trigger from the UI: Visit `http://localhost:8080`, log in with `admin/admin`, unpause `ecommerce_local_e2e` and click Trigger.
- Trigger from CLI (test execution without scheduler):
  ```bash
  data-pipeline/.venv-de/bin/airflow dags test ecommerce_local_e2e 2023-01-01
  ```

**E2E Validation Results:**
The `ecommerce_local_e2e` DAG orchestrates the pipeline from PostgreSQL -> Kafka -> Bronze -> Silver -> Gold and finally runs DuckDB analytical validations to prove GMV correctness, referential integrity, and data quality.
If any validation fails, the `duckdb_validation` task will report failure.

**Troubleshooting:**
- PostgreSQL unavailable: Ensure `docker compose ... --profile backend up` is running.
- Kafka unavailable: Ensure `docker compose ... --profile events up` is running.
- Spark unavailable: Ensure the `data-pipeline/.venv-de` environment is properly set up.
- DAG Import failure: Run `data-pipeline/.venv-de/bin/airflow dags list-import-errors`
- Missing Parquet files / DuckDB failure: Run `airflow dags test` locally to view logs for `bronze_ingestion`, `silver_transformation`, or `gold_transformation` errors.

### Running the Backend Application
The application is a standard Spring Boot 3 app running on Java 21.

To run tests:
```bash
cd backend
./gradlew test
```

To run the application locally (defaults to port 8080):
```bash
cd backend
./gradlew bootRun
```
