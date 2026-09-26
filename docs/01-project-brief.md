# Real-time E-commerce Analytics Platform
## Project Brief & High-Level Scope

---

## 1. Project Overview

The **Real-time E-commerce Analytics Platform** is a dual-purpose software engineering portfolio project designed to showcase proficiency across two complementary disciplines:

1. **Backend Engineering:** Building a robust, maintainable, event-driven transactional e-commerce platform.
2. **Data Engineering:** Designing and implementing an end-to-end data pipeline that ingests, cleanses, models, and serves analytical data using modern lakehouse and data warehousing principles.

Rather than treating Backend and Data Engineering as isolated domains or relying exclusively on static external datasets (e.g., Kaggle dumps), this platform demonstrates how transactional business activities (OLTP) generate high-volume event streams that feed an independent analytical ecosystem (OLAP).

---

## 2. Problem Statement

In modern e-commerce systems, scaling business analytics directly against the transactional database introduces severe bottlenecks:

- **Resource Contention:** Running heavy aggregation and reporting queries on operational databases (e.g., PostgreSQL) locks tables, consumes CPU/memory, and slows down latency-critical customer interactions (checkout, payment).
- **Schema Mismatch:** Transactional systems are optimized for normalized, single-row write operations (3NF), whereas analytical workflows require denormalized, dimensional models (Star/Snowflake schema) optimized for fast aggregations across historical timeframes.
- **Data Evolution & Replayability:** Operational systems capture only the current state of records, losing critical point-in-time state transitions and behavioral telemetry (such as browsing activity or abandoned checkouts).

**Solution:**  
Decouple the operational engine from the analytical engine by establishing an **Event-Driven Architecture (EDA)**. The transactional backend serves customer requests and publishes granular domain events to an event streaming platform (**Apache Kafka**). A dedicated data platform ingests these events, applies a multi-stage **Medallion Architecture (Bronze $\rightarrow$ Silver $\rightarrow$ Gold)**, and populates an analytical data warehouse for high-performance business intelligence without impacting customer experience.

---

## 3. Project Goals

### 3.1 Operational Goals (Backend)
- Provide a functional, clean, and reliable e-commerce REST API handling core user journeys: product catalog browsing, cart operations, order creation, and payment simulation.
- Enforce transactional integrity, business validations, and inventory constraints in the operational database.
- Emit structured, versioned business events to Kafka whenever domain state changes occur.
- Implement caching patterns to optimize read-heavy endpoints and safeguard operational storage.

### 3.2 Analytical Goals (Data Engineering)
- Ingest high-throughput streaming events from Kafka without data loss.
- Implement an automated Medallion pipeline (Bronze $\rightarrow$ Silver $\rightarrow$ Gold) using distributed processing frameworks.
- Cleanse, deduplicate, validate, and conform incoming event payloads.
- Model analytical data into a Star Schema (Fact and Dimension tables) optimized for business queries.
- Orchestrate recurring data batch jobs, transformations, and data quality assertions.

### 3.3 Engineering & Portfolio Goals
- Demonstrate strict adherence to **SOLID principles**, modular design, separation of concerns, and clean coding standards.
- Build a reproducible, local-first environment using Docker containers for seamless testing and demonstration.
- Provide clear documentation, architectural diagrams, and verified test suites to support technical interview discussions.

---

## 4. Target Users

| User Persona | Role & Description | Primary Interaction / Value |
| :--- | :--- | :--- |
| **End Customer** | Shopper browsing the e-commerce store | Interacts with Backend APIs (browse catalog, manage cart, place orders, simulate payments). |
| **Business Analyst / Executive** | Decision-maker requiring timely business metrics | Queries analytical warehouse / dashboards to evaluate revenue, product trends, and conversion funnels. |
| **Data / Backend Engineer (Interviewer / Reviewer)** | Evaluates system architecture and code quality | Inspects API boundaries, event contracts, idempotency, pipeline lineage, and orchestration health. |

---

## 5. Business Context

The simulated business represents an online consumer electronics retailer offering product categories such as:
- Laptops & Desktops
- Smartphones & Tablets
- Audio Devices (Headphones, Speakers)
- Computer Peripherals (Keyboards, Mice, Monitors)
- Accessories & Cables

### Key Business Operations
- **Catalog Management:** Categorized products with attributes, prices, and stock inventory.
- **Shopping Cart Management:** Users aggregate desired items with selected quantities.
- **Checkout & Order Creation:** Converting active cart items into binding orders with calculated line totals, taxes, and shipping estimates.
- **Payment Processing:** Simulating customer payment approval or rejection with immediate order state transitions.
- **Inventory Tracking:** Deducting stock upon order confirmation and reconciling stock upon cancellation.

---

## 6. Core Business Flows

The platform models six fundamental business lifecycles:

```
[ User Registration / Auth ]
            │
            ▼
   [ Product Browsing ] ──────► Emits: PRODUCT_VIEWED
            │
            ▼
    [ Cart Management ] ──────► Emits: CART_UPDATED / ITEM_ADDED
            │
            ▼
   [ Order Placement ]  ──────► Emits: ORDER_CREATED
            │
            ▼
  [ Payment Simulation ] ─────► Emits: PAYMENT_COMPLETED or PAYMENT_FAILED
            │
      ┌─────┴─────────────────┐
      ▼                       ▼
(Order Confirmed)       (Order Cancelled)
Emits: ORDER_PAID       Emits: ORDER_CANCELLED
```

1. **User Sign-up & Authentication:** User registers, authenticates, and retrieves access tokens.
2. **Product Browsing:** User views lists and detailed specifications (generates tracking events).
3. **Cart Operations:** User adds, modifies, or deletes items in their personal cart.
4. **Order Placement:** User initiates checkout. System verifies product availability and records the order in `PENDING` state.
5. **Payment Execution:** Payment simulation service attempts settlement. On success, order transitions to `PAID`; on failure, transitions to `PAYMENT_FAILED` or `CANCELLED`.
6. **Order History & Inspection:** User reviews previous order records, items, and historical fulfillment statuses.

---

## 7. Backend Scope

### 7.1 Core Domains & Entities
- **User Domain:** User accounts, authentication credentials, customer profile attributes.
- **Product & Category Domain:** Hierarchical product classifications, descriptions, pricing, and stock levels.
- **Cart Domain:** Temporary state holding items and quantities prior to order creation.
- **Order Domain:** Order entity containing status, timestamps, pricing breakdown, shipping metadata, and associated `OrderItem` records.
- **Payment Domain:** Payment transaction records linked to orders, tracking settlement status, method, and transaction references.

### 7.2 Core Backend Responsibilities
- Provide RESTful API endpoints adhering to standard HTTP conventions and status codes.
- Manage ACID transactions in the relational operational store.
- Cache high-read data (e.g., active product catalog items) via in-memory caching.
- Construct and publish serialized, structured domain events to designated Kafka topics asynchronously upon successful transactional commitments.
- Implement comprehensive unit and integration testing across controller, service, and repository layers.

---

## 8. Data Engineering Scope

### 8.1 Lakehouse / Medallion Architecture
- **Raw / Bronze Layer:**
  - Persist verbatim event payloads ingested from Kafka topics.
  - Maintain immutability and record lineage (ingestion timestamp, partition, offset, raw payload).
  - Serve as the replayable single source of truth for downstream rebuilds.
- **Cleansed / Silver Layer:**
  - Parse and normalize nested JSON payloads.
  - Clean data types (timestamps, decimals, string sanitization).
  - Filter out invalid records, enforce schema constraints, and deduplicate events based on primary business keys.
- **Curated / Gold Layer:**
  - Denormalize and aggregate business metrics.
  - Structure data into Star Schema dimensional models (Fact and Dimension tables).
  - Compute rollups for revenue, conversion funnels, and operational KPIs.

### 8.2 Ingestion, Orchestration & Serving
- **Stream Ingestion:** Consume continuous event streams from Kafka into the data platform.
- **Batch Processing:** Execute distributed transformations across data tiers.
- **Orchestration:** Coordinate pipeline workflows, schedule transformation cycles, implement retry mechanisms, and enforce dependency order using workflow orchestration.
- **Serving & Warehousing:** Load curated Gold data into an analytical warehouse for fast querying.
- **Data Quality & Testing:** Validate data contracts, record counts, null checks, and freshness thresholds across pipeline stages.

---

## 9. Data Sources

The platform combines four strategic data sources to balance operational authenticity and realistic data scale:

| Source Type | Description | Primary Transport / Format | Role in Platform |
| :--- | :--- | :--- | :--- |
| **Source 1: Operational Database** | Transactional records generated by user interactions. | PostgreSQL (Relational tables) | System of record for day-to-day operations (Users, Catalog, Orders). |
| **Source 2: Domain Events** | Discrete real-time business occurrences emitted by the backend. | Apache Kafka (JSON / Avro - *TBD*) | Real-time event stream (`ORDER_CREATED`, `PAYMENT_COMPLETED`, etc.). |
| **Source 3: Seed / Baseline Data** | Realistic synthetic dataset generated to populate initial catalog and historical customers. | Python / Faker scripts $\rightarrow$ SQL / CSV seeds | Populates baseline products, categories, and past baseline data. |
| **Source 4: Traffic / Load Generator** | Configurable synthetic traffic generator simulating concurrent shoppers and view/order streams. | Python script or worker service producing events | Generates continuous traffic volume to stress-test pipelines and demonstrate realistic analytics. |

---

## 10. High-Level Data Flow

```
                           +------------------------+
                           |  Shoppers / Frontend   |
                           +-----------+------------+
                                       |
                                       | REST API Calls
                                       v
                    +--------------------------------------+
                    |      E-commerce Backend Service      |
                    |            (Spring Boot)             |
                    +---------+------------------+---------+
                              |                  |
              Reads / Writes  |                  | Emits Domain Events
                              v                  v
                      +---------------+  +---------------+
                      |  PostgreSQL   |  | Apache Kafka  |
                      |    (OLTP)     |  | (Event Broker)|
                      +---------------+  +-------+-------+
                                                 |
                       +-------------------------+
                       | Ingestion (Batch / Streaming)
                       v
         +----------------------------+
         |    Bronze Layer (Raw)      | <--- Immutably stores raw payloads
         +-------------+--------------+
                       |
                       | PySpark Processing (Cleanse, Validate, Deduplicate)
                       v
         +----------------------------+
         |   Silver Layer (Cleaned)   | <--- Normalized records, typed schemas
         +-------------+--------------+
                       |
                       | PySpark Aggregation & Dimensional Modeling
                       v
         +----------------------------+
         |    Gold Layer (Curated)    | <--- Star Schema (Fact & Dimension tables)
         +-------------+--------------+
                       |
                       | Warehouse Load / Serving
                       v
         +----------------------------+
         |       Data Warehouse       | <--- Queryable Analytical Store
         +-------------+--------------+
                       |
                       v
         +----------------------------+
         |   BI / Analytics Queries   | <--- Business Reports & Insights
         +----------------------------+
                       ^
                       | Monitored & Scheduled by
         +-------------+--------------+
         |       Apache Airflow       | <--- Workflow Orchestration & Quality Gates
         +----------------------------+
```

---

## 11. Business Questions

The analytical tier must provide actionable answers to high-level strategic questions across four core dimensions:

### 11.1 Sales & Revenue Performance
- What is the total Gross Merchandise Value (GMV) aggregated daily, weekly, and monthly?
- What is the Average Order Value (AOV) across the entire platform and broken down by category?
- What is the volume and distribution of orders across different order statuses?

### 11.2 Product & Catalog Intelligence
- Which products generate the highest gross revenue and sales volume?
- What are the fastest-growing and top-performing product categories?
- Which products exhibit high view counts but disproportionately low purchase conversions (identifying pricing or description friction)?

### 11.3 Customer Demographics & Behavior
- How many unique purchasing customers were active over a given timeframe?
- Who are the top 10% highest-value customers by total lifetime spend (LTV)?
- What is the platform's repeat purchase rate?

### 11.4 Operational & Data Pipeline Health
- What proportion of orders are cancelled, and what is the primary failure stage (e.g., checkout abandonment vs. payment gateway failure)?
- What is the success-to-failure ratio of payment transactions?
- Are all published Kafka events accounted for in the Gold layer without duplication or silent dropouts (End-to-End Lineage & Completeness)?
- What is the data latency between transactional event creation and Gold analytical availability?

---

## 12. Portfolio Objectives

The platform is intentionally calibrated to showcase verifiable competency across both target roles:

### 12.1 Backend Engineering Competencies
- **Architecture & Design:** Clean modular design, clear separation of domain layers, and strict application of SOLID principles.
- **RESTful API Engineering:** Idiomatic REST design, validation, global exception handling, and standard status signaling.
- **Data Persistence & Caching:** Relational modeling, transaction demarcation, query optimization, and read-side caching (e.g., Redis).
- **Event-Driven Integration:** Safe event emission using reliable publishing patterns to message brokers (Kafka).
- **Quality Assurance:** Comprehensive testing strategy including unit tests and integration tests with test containers.

### 12.2 Data Engineering Competencies
- **Data Lakehouse Paradigm:** Practical application of the Medallion Architecture (Bronze, Silver, Gold).
- **Distributed Computing:** PySpark transformations, schema enforcement, deduplication, window functions, and partitioning.
- **Dimensional Modeling:** Designing business-centric Star Schemas (Fact and Dimension tables, surrogate keys, slowly changing concepts).
- **Pipeline Orchestration:** Building modular, testable, and idempotent Apache Airflow DAGs with retry policies.
- **Data Reliability & Governance:** Data validation checks, completeness auditing, and failure alerting.

---

## 13. High-Level Technology Direction

*Note: Specific library versions, serialization protocols, and secondary tooling will be finalized during technical design.*

| Component Layer | Technology Candidate | Purpose / Responsibility | Status |
| :--- | :--- | :--- | :--- |
| **Backend Language & Framework** | Java (17 / 21) & Spring Boot 3.x | Operational business logic, domain services, REST endpoints. | Selected |
| **Operational Database** | PostgreSQL | Transactional database (OLTP) for user, product, order, and payment records. | Selected |
| **Cache Layer** | Redis | In-memory cache for catalog reads and session/cart acceleration. | Selected |
| **Event Streaming Broker** | Apache Kafka | Event bus for publishing and distributing business events. | Selected |
| **Data Processing Engine** | Python & Apache Spark (PySpark) | Batch and/or streaming transformations across Bronze, Silver, and Gold. | Selected |
| **Pipeline Orchestrator** | Apache Airflow | Scheduling, DAG dependencies, retry handling, and data pipeline orchestration. | Selected |
| **Data Warehouse / Analytical Store** | BigQuery / ClickHouse / DuckDB | Analytical OLAP store housing the Gold dimensional models. | *To be decided during architecture design* |
| **Storage Layer (Bronze/Silver/Gold)** | Local Filesystem / MinIO / S3 (Parquet/Delta) | Storage format and persistence medium for lakehouse tiers. | *To be decided during architecture design* |
| **Data Quality Framework** | Great Expectations / Soda / Spark Asserts | Automated validation of event schemas, nullability, and metric thresholds. | *To be decided during architecture design* |
| **Analytics & Visualization** | Superset / Metabase / Streamlit / SQL Scripts | Visual dashboards or verified SQL reporting scripts answering business questions. | *To be decided during architecture design* |
| **Containerization & Dev Environment** | Docker & Docker Compose | Multi-container local orchestration for local evaluation and demonstration. | Selected |

---

## 14. Non-Goals

To maintain high technical depth, clear architectural focus, and realistic delivery boundaries, the following areas are explicitly **out of scope**:

- **No Microservices Sprawl:** The backend will not be fragmented into dozens of microservices requiring service meshes or distributed tracing overhead. A clean, modular monolith with clear domain boundaries is chosen to prioritize domain clarity and local maintainability.
- **No Kubernetes / Multi-Cloud Orchestration:** The project will not use Kubernetes, Helm, or multi-cloud infrastructure orchestration that distracts from core software and data engineering logic.
- **No Heavyweight Auxiliary Engines:** Technologies such as Apache Flink, Elasticsearch, MongoDB, or Cassandra will not be introduced without strict architectural necessity.
- **No Real Third-Party Payment / Shipping Integrations:** Financial transactions and logistics will be simulated realistically within the domain logic rather than bound to live sandbox APIs (e.g., Stripe, PayPal, FedEx).
- **No Enterprise Frontend:** While basic client interaction or API documentation (e.g., Swagger/OpenAPI, simple UI) may exist, building an intricate production React/Vue customer-facing web application is not a primary objective.

---

## 15. Project Constraints

1. **Local-First Executability:** The entire core platform (Backend, Postgres, Redis, Kafka, Spark, Airflow) must be runnable on a single standard development machine (e.g., 16 GB RAM) via Docker Compose.
2. **Cost-Free Operation:** The design must avoid mandatory paid cloud infrastructure dependencies. Where cloud technologies (such as BigQuery) are supported, local functional equivalents or emulations (such as DuckDB, ClickHouse, or PostgreSQL OLAP instances) must be provided.
3. **Architectural Decoupling:** Under no circumstances may data transformation pipelines query the operational PostgreSQL database directly for analytical reporting. All analytical data must flow through event streams or designated ingestion paths.
4. **Reproducibility:** Setting up, seeding data, executing the pipeline, and running analytical queries must be automated through reproducible scripts and clear documentation.

---

## 16. Success Criteria

The project will be considered successful when:

- [ ] **Functional Operational Flow:** A user can register, browse products, add items to a cart, check out, and settle an order via API, with state correctly persisted in PostgreSQL.
- [ ] **Reliable Event Emission:** Every key business operation triggers a corresponding structured event in Kafka (`USER_REGISTERED`, `ORDER_CREATED`, `PAYMENT_COMPLETED`, etc.) without impacting API response times.
- [ ] **Functional Medallion Pipeline:** PySpark jobs successfully ingest raw Kafka event dumps into Bronze, cleanse and deduplicate them into Silver, and compute Star Schema models in Gold.
- [ ] **Automated Orchestration:** An Apache Airflow DAG automates the processing sequence, enforces task dependencies, and executes data quality assertions.
- [ ] **Business Questions Answered:** Analytical SQL queries run against the Gold layer accurately answer all core business questions defined in Section 11.
- [ ] **Code Quality & Testing:** High unit/integration test coverage for critical backend services; modular, PEP8-compliant PySpark transformations with automated test cases.
- [ ] **Zero Unexplained Dependencies:** Every tool and library in the stack has an articulated rationale directly supporting backend or data engineering objectives.

---

## 17. Future Extensions

Potential post-MVP enhancements to explore once the core platform is established:

- **Change Data Capture (CDC):** Introducing Debezium to stream row-level database changes directly from PostgreSQL WAL logs into Kafka.
- **Real-Time Stream Processing:** Implementing PySpark Structured Streaming or lightweight stream consumers for sub-second KPI aggregation.
- **Real-Time Anomaly & Fraud Detection:** Streaming rule engine monitoring payment failures and suspicious purchase frequencies.
- **Cloud-Native Deployment:** Deploying the pipeline to managed cloud services (e.g., AWS EMR / GCP Dataproc, BigQuery, Managed Airflow).
