# Real-time E-commerce Analytics Platform
## Requirements Specification

---

## 1. Requirements Overview

### 1.1 Purpose
This document provides the formal **Requirements Specification** for the **Real-time E-commerce Analytics Platform**. It translates the high-level business goals, architectural visions, and domain context established in [`01-project-brief.md`](file:///home/nguyen-son-minh/Downloads/Real-time%20E-commerce%20Analytics%20Platform/docs/01-project-brief.md) into concrete, unambiguous, and verifiable technical requirements.

### 1.2 Relationship to Other Project Documents
This document serves as the single source of truth for functional behavior, data contracts, and quality standards, acting as the bridge between conceptual direction and technical design:

```
[ 01-project-brief.md ] (High-level vision & scope)
         │
         ▼
[ 02-requirements.md ]  (Concrete, testable specifications - THIS DOCUMENT)
         │
         ├──────────────────────────┬──────────────────────────┐
         ▼                          ▼                          ▼
[ 03-use-cases.md ]        [ 04-system-architecture.md ] [ 05-database-design.md ]
(Actor workflows)          (Component topologies)        (OLTP schema & ACID)
         │                          │                          │
         ▼                          ▼                          ▼
[ 08-api-specification.md ][ 07-data-pipeline.md ]       [ 06-data-model.md ]
(REST contracts)           (ETL/ELT & Orchestration)     (Star Schema / OLAP)
```

### 1.3 Scope of Specification
- **What is covered:** Detailed functional behaviors across all business domains, event emission standards, data lifecycle stages (Bronze, Silver, Gold), business rules, non-functional quality attributes, idempotency guarantees, and acceptance criteria.
- **What is deferred:** Low-level implementation details, physical database schemas (DDL), exact REST endpoint paths, Kafka partition tuning, Airflow DAG code, and specific library version locks. Those will be detailed in their dedicated design documents.

---

## 2. Scope

### 2.1 In Scope
1. **Core Transactional E-commerce Backend:**
   - User registration, authentication, and session identity.
   - Product catalog browsing, filtering, and inventory status tracking.
   - User shopping cart management (add, update, delete, total derivation).
   - Checkout processing, order placement, and inventory reservation.
   - Payment simulation supporting deterministic success and failure branches.
   - Order lifecycle tracking (status progression and cancellations).
2. **Event-Driven Messaging:**
   - Reliable publication of business events upon transactional state changes.
   - Asynchronous decoupled delivery to a messaging broker (Apache Kafka).
3. **Data Platform & Lakehouse Processing:**
   - Batch and/or streaming ingestion of raw event streams into immutable storage (Bronze).
   - Distributed cleansing, schema enforcement, and deduplication (Silver).
   - Dimensional modeling (Star Schema) and analytical aggregations (Gold).
   - Data quality validation checks and error quarantine.
4. **Orchestration & Analytics:**
   - Scheduled workflow execution, dependency management, and retry handling (Apache Airflow).
   - Analytical query serving addressing key business metrics (sales, conversion, customer LTV).
5. **Engineering & Demonstration:**
   - Containerized multi-service local environment (Docker Compose).
   - Synthetic seed data scripts and configurable traffic simulation.
   - Comprehensive testing suite (unit, integration, pipeline verification).

### 2.2 Out of Scope
1. **Third-Party External Integrations:** Live payment provider APIs (Stripe, PayPal) and real logistics carrier APIs (FedEx, DHL).
2. **Infrastructure Complexity:** Kubernetes cluster orchestration, Helm charts, Service Meshes (Istio), and multi-region cloud provisioning.
3. **Microservices Sprawl:** Decomposing the backend into numerous standalone microservices; a modular monolith architecture is enforced.
4. **Specialized Search/NoSQL Engines:** Elasticsearch, Solr, MongoDB, Cassandra, or Apache Flink.
5. **Full-Featured Consumer Frontend:** A production-grade multi-page SPA/PWA web store is excluded; interactions will be validated via REST APIs, automated tests, and basic UI/Swagger interfaces.

---

## 3. Functional Requirements

### 3.1 User Management
- **`FR-001` - User Registration:** The system must allow new users to register by providing a unique identifier (email/username), a secure password, and profile metadata (full name).
- **`FR-002` - User Authentication:** The system must authenticate registered users using valid credentials and issue an authentication token for subsequent authorized requests.
- **`FR-003` - User Profile Retrieval:** The system must allow authenticated users to view their account details and registration status.
- **`FR-004` - User Event Emission:** The system must emit a structured `USER_REGISTERED` event to the event broker upon successful user account creation.

### 3.2 Product and Category Management
- **`FR-005` - Category Hierarchy:** The system must organize products into distinct, queryable categories (e.g., Laptops, Audio, Peripherals).
- **`FR-006` - Catalog Browsing:** The system must provide endpoints to list active products with pagination, category filtering, and basic sorting (e.g., price, date).
- **`FR-007` - Product Detail Inspection:** The system must allow users to view detailed specifications, current pricing, and inventory availability for a specific product.
- **`FR-008` - Product View Tracking:** The system must record product browsing interactions and emit a `PRODUCT_VIEWED` event to capture customer interest telemetry.

### 3.3 Cart Management
- **`FR-009` - Cart Lifecycle:** The system must maintain an active shopping cart tied to an authenticated user or active guest session.
- **`FR-010` - Add Item to Cart:** The system must allow users to add an available product with a specified positive quantity to their cart, verifying stock availability.
- **`FR-011` - Update Cart Item Quantity:** The system must allow users to adjust the quantity of an item already in the cart, recalculating line totals.
- **`FR-012` - Remove Item from Cart:** The system must allow users to remove individual items from their cart or clear the cart entirely.
- **`FR-013` - Cart Total Derivation:** The system must dynamically calculate and display the cart subtotal based on current item unit prices and selected quantities.
- **`FR-014` - Cart Event Emission:** The system must emit a domain event (`CART_UPDATED` or `ITEM_ADDED`) whenever cart contents are modified.

### 3.4 Order Management
- **`FR-015` - Order Placement:** The system must allow an authenticated user to convert the current active cart into an immutable order in `PENDING` state.
- **`FR-016` - Historical Price Snapshotting:** The system must capture an immutable snapshot of each product's price, title, and quantity at the exact moment of order placement.
- **`FR-017` - Inventory Reservation:** The system must validate and decrement available stock when an order is created, preventing overselling.
- **`FR-018` - Order Total Calculation:** The order total must be calculated accurately as the sum of order item line amounts plus applicable taxes and shipping fees.
- **`FR-019` - Order Status Lifecycle:** The system must manage order state transitions according to a defined state machine:
  - From `PENDING`: An order may transition to `PAID` (on successful payment), `PAYMENT_FAILED` (on payment rejection), or `CANCELLED` (on cancellation or timeout).
  - From `PAYMENT_FAILED`: An order may retry payment to become `PAID` or transition to `CANCELLED`.
  - From `PAID`: An order may transition to `COMPLETED` (on fulfillment).
  - Terminal states (`CANCELLED`, `COMPLETED`) cannot transition to any other status.
- **`FR-020` - Order Cancellation:** The system must allow cancellation of unpaid orders, restoring reserved inventory back to available stock.
- **`FR-021` - Order History Inspection:** The system must allow users to retrieve their past orders with order status, timestamps, and itemized breakdown.
- **`FR-022` - Order Event Emission:** The system must emit corresponding events (`ORDER_CREATED`, `ORDER_PAID`, `ORDER_CANCELLED`) upon each respective order state transition.

### 3.5 Payment Simulation
- **`FR-023` - Payment Initiation:** The system must initiate a payment attempt against a specific order currently in `PENDING` status.
- **`FR-024` - Deterministic Payment Simulation:** The simulation engine must support configurable outcomes (successful settlement or specific failure scenarios such as insufficient funds or expired card).
- **`FR-025` - Payment Success Processing:** On payment success, the system must record a completed payment transaction, transition the order status to `PAID`, and emit `PAYMENT_COMPLETED`.
- **`FR-026` - Payment Failure Processing:** On payment failure, the system must record the failed transaction with an error code, maintain or transition order status to `PAYMENT_FAILED`, and emit `PAYMENT_FAILED`.
- **`FR-027` - Payment Record Association:** Every payment record must maintain a strict 1:1 or 1:N relationship with its associated order, preserving an audit trail of attempts.

---

## 4. Event and Messaging Requirements

- **`ER-001` - Business Event Significance:** Events published to the message broker must represent distinct, significant domain occurrences:
  - `USER_REGISTERED`
  - `PRODUCT_VIEWED`
  - `CART_UPDATED`
  - `ORDER_CREATED`
  - `PAYMENT_COMPLETED`
  - `PAYMENT_FAILED`
  - `ORDER_PAID`
  - `ORDER_CANCELLED`
- **`ER-002` - Self-Contained Payloads:** Event payloads must contain all contextual data required for analytical processing (entity identifiers, timestamps, numerical amounts, statuses, and changed attributes) without requiring consumers to query the operational database.
- **`ER-003` - Common Envelope Structure:** Every emitted event must adhere to a standardized envelope metadata format containing:
  - `event_id`: Unique UUID for traceability and deduplication.
  - `event_type`: Categorical name of the business event.
  - `event_timestamp`: UTC timestamp when the event occurred in ISO-8601 format.
  - `producer`: Identifier of the originating system/service.
  - `payload`: Domain-specific JSON payload.
- **`ER-004` - Reliable Event Generation:** The system must prevent loss of critical business events when transactional operations succeed. The publishing mechanism must decouple transactional operations from broker latency. The concrete consistency and dispatch mechanisms (e.g., direct asynchronous dispatch, transactional outbox pattern, or fallback buffering) are deferred to system architecture design (`04-system-architecture.md`).
- **`ER-005` - Serialization Format:** Events must be serialized using a machine-readable, schema-compatible format (*JSON or Avro - TBD during technical design*).
- **`ER-006` - Independent Consumer Processing:** The data platform must ingest and process messages from Kafka completely independently of backend operational cycles.

---

## 5. Data Requirements

### 5.1 Operational Data (OLTP)
The operational database (PostgreSQL) must store normalized, transactional data with full ACID guarantees:
- **`DR-001` - User Entities:** User identifiers, authentication credentials (salted/hashed), contact info, created/updated timestamps.
- **`DR-002` - Catalog Entities:** Categories and products including SKU, title, description, unit price (numeric precision), active status, and inventory stock level.
- **`DR-003` - Cart Entities:** Cart identifier, user identifier, item references, quantities, and last-updated timestamps.
- **`DR-004` - Order Entities:** Order identifier, user reference, order status, subtotal, tax, shipping, total amount, and line item details (`OrderItem` containing product snapshot and purchase price).
- **`DR-005` - Payment Entities:** Payment transaction identifier, order reference, payment method, payment status, transaction reference, amount, and timestamp.

### 5.2 Event Data (Streaming / Ingestion)
- **`DR-006` - Immutability:** Event records, once produced and committed to the log, are strictly append-only and cannot be updated or deleted in place.
- **`DR-007` - Event Ordering Requirement:** Events must preserve chronological ordering where business state transitions require sequential processing (e.g., entity state changes). The specific topic topology, message keys, and partitioning strategy are deferred to system architecture design (`04-system-architecture.md`).
- **`DR-008` - Retention & Durability:** The messaging broker must retain raw event streams for a configurable time window (e.g., minimum 7 days) to support replayability and disaster recovery.

---

## 6. Data Engineering Requirements

```
[ Kafka Stream ] ──► [ Bronze Layer ] ──► [ Silver Layer ] ──► [ Gold Layer ] ──► [ Data Warehouse ]
                       (Raw / Audit)       (Clean / Dedup)     (Star Schema)        (OLAP Queries)
```

### 6.1 Bronze Layer (Raw Ingestion)
- **`DER-001` - Verbatim Raw Ingestion:** Ingest event streams directly into Bronze storage preserving the exact raw payload as received from Kafka.
- **`DER-002` - Ingestion Metadata Enrichment:** Append technical metadata to each raw record:
  - `ingestion_timestamp`: UTC time when the record was written to Bronze.
  - `source_topic`: Originating Kafka topic name.
  - `kafka_partition`: Broker partition number.
  - `kafka_offset`: Exact broker message offset.
- **`DER-003` - Full Replayability:** The Bronze tier must serve as the immutable single source of truth capable of replaying the entire analytical dataset from scratch.
- **`DER-004` - Non-Destructive Ingestion:** No data transformation or schema-validation failure at ingestion time may result in discarding or altering raw records.

### 6.2 Silver Layer (Cleansing & Conformance)
- **`DER-005` - Schema Enforcement & Type Casting:** Parse raw payloads, flatten nested structures, cast strings to strongly typed primitives (integers, decimals, timestamps), and validate required fields.
- **`DER-006` - Deduplication:** Detect and eliminate duplicate events originating from network retries or producer re-emissions using `event_id` and business key combinations.
- **`DER-007` - Data Sanitization:** Trim whitespace, standardize currency scales, normalize status values to uppercase, and enforce date formatting standards.
- **`DER-008` - Conformed Business Tables:** Output clean, queryable tabular datasets representing core business entities (e.g., `silver_orders`, `silver_order_items`, `silver_payments`, `silver_product_views`).
- **`DER-009` - Bad Record Isolation:** Automatically route malformed, unparseable, or invalid records into a dedicated quarantine store without terminating the pipeline.

### 6.3 Gold Layer (Curated Dimensional Models)
- **`DER-010` - Star Schema Structure:** Transform conformed Silver data into business-ready dimensional models comprising:
  - **Fact Tables:** Quantitative business events (e.g., `fact_orders`, `fact_order_items`, `fact_payments`, `fact_product_views`).
  - **Dimension Tables:** Contextual descriptive attributes (e.g., `dim_products`, `dim_categories`, `dim_customers`, `dim_date`, `dim_order_status`).
- **`DER-011` - Pre-aggregated Business Metrics:** Compute rollups and summary tables (e.g., daily sales, category revenue, conversion rates) to accelerate analytical dashboards.
- **`DER-012` - Warehouse Synchronization:** Synchronize curated Gold tables into the designated analytical data warehouse (*BigQuery / ClickHouse / DuckDB - TBD during technical design*).

---

## 7. Data Quality Requirements

- **`DQR-001` - Completeness:** Primary keys and essential foreign keys (`order_id`, `user_id`, `product_id`) as well as timestamps must have a 0% null rate across Silver and Gold tables.
- **`DQR-002` - Uniqueness:** Primary business keys in Silver conformed tables and Gold dimension tables must be 100% unique; duplicate records must be detected and resolved.
- **`DQR-003` - Value Validity:**
  - Monetary fields (`unit_price`, `total_amount`, `tax`) must be strictly non-negative ($\ge 0$).
  - Item quantities must be strictly positive integers ($> 0$).
  - Timestamps must fall within realistic chronological boundaries (not in the future, not older than platform inception).
  - Status strings must conform strictly to predefined enumeration sets.
- **`DQR-004` - Referential Consistency:** Every line item in `fact_order_items` must resolve to a valid product in `dim_products` and a parent order in `fact_orders`.
- **`DQR-005` - Automated Quality Gate Enforcement:** Automated quality checks must execute at the boundary between Silver and Gold. If validation error rates exceed defined thresholds (e.g., $> 1\%$ critical data errors), downstream publishing must halt and trigger an alert.
- **`DQR-006` - Auditability:** All quality check runs, records evaluated, pass/fail counts, and quarantined record references must be logged for inspection.

---

## 8. Business Rules

### 8.1 Product & Catalog Rules
- **`BR-001` - Product Existence:** A product must exist and be in active status before it can be viewed, added to a cart, or referenced in an order.
- **`BR-002` - Historical Price Protection:** Changes to a product's current catalog price must never alter the price recorded on existing or historical orders.
- **`BR-003` - Inventory Non-Negativity:** Product inventory quantity cannot fall below zero. An order request exceeding available stock must be rejected.

### 8.2 Cart Rules
- **`BR-004` - Quantity Bounds:** Cart item quantities must be greater than zero and cannot exceed configured per-order purchasing limits (e.g., max 100 units per line item).
- **`BR-005` - Dynamic Total Derivation:** Cart subtotal must always equal the sum of each item's current unit price multiplied by its quantity:
  $$\text{Cart Subtotal} = \sum (\text{item\_price} \times \text{quantity})$$

### 8.3 Order Rules
- **`BR-006` - Non-Empty Orders:** An order must contain at least one item to be valid.
- **`BR-007` - Legal Status Progression:** An order's status may only advance according to valid state machine transitions:
  - `PENDING` $\rightarrow$ `PAID` (on successful payment), `PAYMENT_FAILED` (on payment rejection), or `CANCELLED` (on customer cancellation or inventory timeout).
  - `PAYMENT_FAILED` $\rightarrow$ `PAID` (upon successful retry attempt) or `CANCELLED` (upon cancellation).
  - `PAID` $\rightarrow$ `COMPLETED` (on successful fulfillment).
  - Terminal states (`CANCELLED`, `COMPLETED`) cannot transition to any other status.
- **`BR-008` - Inventory Restoration:** When an order transitions to `CANCELLED`, any inventory reserved for that order must be immediately credited back to available stock.

### 8.4 Payment Rules
- **`BR-009` - Order Association:** Every payment transaction must link to a valid order in `PENDING` state.
- **`BR-010` - Paid Order Immunity:** An order that has already reached `PAID` status cannot accept additional payment attempts.
- **`BR-011` - Failed Payment Isolation:** A failed payment attempt must never fulfill an order or grant access to purchased items.

### 8.5 Analytics Business Rules
- **`BR-012` - Recognized Revenue Definition:** Gross Merchandise Value (GMV) and revenue calculations must include **only** orders with status `PAID` or `COMPLETED`.
- **`BR-013` - Exclusion of Cancelled Orders:** Orders in `CANCELLED` or `PENDING` status must be excluded from gross sales metrics and tracked under operational loss/abandonment metrics.
- **`BR-014` - Idempotent Aggregate Derivation:** Aggregated metric tables in Gold must be computed such that duplicate input events or re-executed pipeline runs produce identical metric outputs.

---

## 9. Non-Functional Requirements

### 9.1 Performance
*(Note: Metrics below define target benchmarks for a standard local development environment with 16 GB RAM, serving as verification baselines rather than production enterprise SLAs).*
- **`NFR-001` - API Response Benchmark:** 
  - Catalog browsing and cart operations target a p95 response time $< 200\text{ ms}$ under standard local test workloads.
  - Checkout and order placement transactions target a p95 response time $< 500\text{ ms}$ under standard local test workloads.
- **`NFR-002` - Event Publishing Benchmark:** Event emission should introduce minimal latency overhead to the primary request thread (target benchmark $< 20\text{ ms}$ locally).
- **`NFR-003` - Batch Processing Benchmark:** Transformation batches over typical local synthetic test datasets (e.g., $\sim 100,000$ events) target completion within approximately $5\text{ minutes}$.
- **`NFR-004` - Analytical Query Responsiveness:** Standard business analytical queries run against Gold warehouse tables target responsive execution (benchmark $< 2\text{ seconds}$ locally).

### 9.2 Reliability & Fault Tolerance
- **`NFR-005` - Graceful Service Degradation:** In the event of temporary cache or message broker unavailability, the backend must degrade gracefully (e.g., logging errors, utilizing fallback queues) without dropping database ACID transactions.
- **`NFR-006` - Pipeline Self-Healing:** Failed Airflow DAG tasks must automatically retry up to 3 times with exponential backoff before signaling failure.
- **`NFR-007` - Pipeline Resumption:** Data pipelines must be capable of resuming execution from the last successful partition or checkpoint without corrupting downstream tables.

### 9.3 Scalability
- **`NFR-008` - Event Stream Scalability:** Event topics and consumers must support horizontal scalability through partition-based distribution.
- **`NFR-009` - Distributed Processing Scalability:** Data transformation jobs must use distributed data structures (PySpark DataFrames) partitioned by date and hour to scale linearly with increasing data volume.

### 9.4 Maintainability & Architecture Quality
- **`NFR-010` - SOLID & Modular Monolith:** Backend architecture must follow SOLID principles, clean domain boundaries, and dependency inversion, avoiding god classes and tightly coupled packages.
- **`NFR-011` - Separation of Concerns:** Database transactions, business validations, REST controller mappings, and event dispatchers must reside in separate architectural layers.
- **`NFR-012` - Code Consistency & Styling:** Java backend code must adhere to standard style guides (Google Java Style); Python scripts and PySpark code must adhere strictly to PEP8 standards.

### 9.5 Observability & Logging
- **`NFR-013` - Structured Logging:** Backend and pipeline applications must output structured, machine-readable logs (JSON format) containing timestamps, log levels, correlation IDs, and context.
- **`NFR-014` - Pipeline Operational Telemetry:** Pipeline runs must log record throughput, processing duration, partition boundaries, and dropped record counts per execution cycle.
- **`NFR-015` - Event Traceability:** Each event must carry a unique `event_id` and origin timestamp to facilitate end-to-end tracing from backend generation to Gold table persistence.

### 9.6 Security
- **`NFR-016` - Password Hashing:** User passwords must never be stored in plaintext and must be salted and hashed using standard algorithms (e.g., BCrypt).
- **`NFR-017` - Stateless Authentication:** Protected endpoints must authenticate incoming requests via stateless security tokens.
- **`NFR-018` - Input Sanitization:** All incoming API parameters and payloads must be validated and sanitized against SQL injection and cross-site scripting attacks.
- **`NFR-019` - Secret Separation:** Database credentials, broker addresses, and secret keys must be injected via environment variables and never committed to source repositories.

### 9.7 Reproducibility & Resource Constraints
- **`NFR-020` - Local Resource Constraint:** The entire system stack (Postgres, Redis, Kafka, Spark, Airflow, Backend) must be capable of running simultaneously on a standard developer workstation with **16 GB RAM**.
- **`NFR-021` - Containerized Bootstrap:** The complete environment must be launchable via Docker Compose with automated service dependency wiring and health checks.
- **`NFR-022` - Automated Seeding:** The project must include automated scripts to seed the database and populate initial historical data with a single command.

---

## 10. Data Reliability and Idempotency Requirements

- **`DRIR-001` - At-Least-Once Delivery Resilience:** The data ingestion and processing pipelines must assume at-least-once delivery semantics from Kafka. Duplicate messages must be handled gracefully without corrupting Silver or Gold records.
- **`DRIR-002` - Idempotent Pipeline Executions:** Re-running an Airflow DAG for a specific historical partition (e.g., processing date `2026-09-25`) must produce identical outputs without creating duplicate rows or inflating metrics.
- **`DRIR-003` - Atomic Partition Writes:** Writes to Silver and Gold tables must use atomic partition overwrites or upsert/merge mechanisms to ensure failed runs leave no partial or corrupt state.
- **`DRIR-004` - Lakehouse Reconstruction:** The system must support wiping Silver and Gold tiers and reconstructing them entirely from the raw immutable Bronze event store (Full Recompute Guarantee).

---

## 11. Authentication and Authorization Requirements

- **`AAR-001` - Public Operations:** Unauthenticated clients must be permitted to:
  - Register a new account.
  - Authenticate (login).
  - Browse product categories, lists, and view product details.
- **`AAR-002` - Authenticated Customer Operations:** Authenticated users must be permitted to:
  - Manage their own shopping cart.
  - Place orders and simulate payments on their own orders.
  - View their own order history and profile information.
- **`AAR-003` - Data Isolation:** Users must never be able to view, modify, or cancel orders or carts belonging to other users.
- **`AAR-004` - Administrative Operations:** Administrative capabilities (e.g., adding/modifying product catalog items or viewing system-wide analytics) must require elevated administrative authorization.

---

## 12. Analytics Requirements

The analytical tier must satisfy the core business questions defined in [`01-project-brief.md`](file:///home/nguyen-son-minh/Downloads/Real-time%20E-commerce%20Analytics%20Platform/docs/01-project-brief.md) through pre-built dimensional queries and metrics:

### 12.1 Sales & Revenue Metrics
- **`AR-001` - Gross Merchandise Value (GMV):** Calculate total recognized revenue aggregated daily, weekly, and monthly.
- **`AR-002` - Average Order Value (AOV):** Compute total revenue divided by total completed orders overall and grouped by product category.
- **`AR-003` - Order Volume by Status:** Track count and percentage distribution of orders across `PENDING`, `PAYMENT_FAILED`, `PAID`, `CANCELLED`, and `COMPLETED`.

### 12.2 Product & Catalog Metrics
- **`AR-004` - Top Products:** Identify top 10 best-selling products by total gross revenue and unit volume over configurable time windows.
- **`AR-005` - Category Revenue Share:** Calculate percentage breakdown of total revenue generated by each product category.
- **`AR-006` - Conversion Funnel Drop-off:** Compare unique product views against cart additions and completed purchases to isolate high-traffic products with low purchase conversion.

### 12.3 Customer Demographics & Behavior
- **`AR-007` - Active Purchasing Customers:** Measure the count of unique distinct customers placing orders within given reporting periods.
- **`AR-008` - Customer Lifetime Value (LTV):** Rank customers based on cumulative historical spending to identify top tier VIP purchasers.
- **`AR-009` - Repeat Purchase Rate:** Calculate the proportion of customers with two or more completed orders compared to single-order customers.

### 12.4 Operational & Platform Health Metrics
- **`AR-010` - Order Cancellation Rate:** Measure the ratio of cancelled orders to total created orders, grouped by cancellation reason or stage.
- **`AR-011` - Payment Settlement Rate:** Calculate the success-to-failure ratio of simulated payment transactions.
- **`AR-012` - Pipeline Event Completeness:** Compare total event counts published by the backend against total unique events recorded in the Gold fact tables to verify zero silent data loss.
- **`AR-013` - Event-to-Gold Data Latency:** Measure the time duration between an event's initial business timestamp and its availability in Gold tables.

---

## 13. Data Source Requirements

- **`DSR-001` - Operational System of Record:** The operational PostgreSQL database serves as the sole authoritative source of truth for real-time customer state, catalog inventory, and order transactional data.
- **`DSR-002` - Analytics Ingestion Decoupling:** The operational PostgreSQL database serves as the transactional system of record, while the Kafka event stream serves as the primary decoupled ingestion path for analytics. To prevent resource contention and locks on the operational database, analytical pipelines and BI queries must not query the operational PostgreSQL database directly. (Any reference or dimension data synchronization paths from the operational store will be defined during architecture design).
- **`DSR-003` - Baseline Data Seeding:** The platform must provide an automated synthetic data seeding script (using Python / Faker) to generate realistic baseline categories, products, user accounts, and historical orders.
- **`DSR-004` - Configurable Traffic Generator:** The platform must provide a standalone traffic generator capable of generating simulated user activity (e.g., $10\text{ views/sec}$, $2\text{ orders/sec}$) to evaluate pipeline throughput and verify real-time analytics.

---

## 14. Testing Requirements

### 14.1 Backend Testing
- **`TR-001` - Unit Testing:** Unit tests must cover all core business logic, domain models, validations, cart calculations, and order status state machine rules with high branch coverage.
- **`TR-002` - Repository & Transaction Testing:** Integration tests must verify database constraints, cascading behaviors, and ACID transactional rollback on runtime exceptions (e.g., using Testcontainers).
- **`TR-003` - API Integration Testing:** Web layer tests must verify HTTP status codes, payload serialization, validation error formats, and authentication filters.

### 14.2 Event System Testing
- **`TR-004` - Serialization & Schema Verification:** Automated tests must verify that domain event payloads conform strictly to the required envelope and domain schemas.
- **`TR-005` - Event Dispatch Verification:** Integration tests must confirm that successful business operations trigger the expected event messages to Kafka topics, and rolled-back operations do not publish events.

### 14.3 Data Pipeline Testing
- **`TR-006` - PySpark Transformation Testing:** Transformation functions across Bronze $\rightarrow$ Silver $\rightarrow$ Gold must be accompanied by unit tests utilizing mock DataFrames to verify schema casting, filtering, and aggregation logic.
- **`TR-007` - Deduplication Verification:** Test suites must submit duplicate event batches and verify that Silver conformed outputs contain exactly one clean record per business key.
- **`TR-008` - Airflow DAG Integrity Testing:** DAG definition files must be validated for syntax errors, cyclic dependencies, missing parameters, and correct task dependencies.

---

## 15. Acceptance Criteria

The system will be evaluated against the following concrete acceptance criteria for MVP delivery:

- **`AC-001` - User Journey Verification:** An end-to-end transactional scenario (User registers $\rightarrow$ browses product $\rightarrow$ adds item to cart $\rightarrow$ creates order $\rightarrow$ completes payment) succeeds via REST APIs with state verified in PostgreSQL.
- **`AC-002` - Event Emission Verification:** The transactional flow in `AC-001` triggers corresponding events in Kafka (`USER_REGISTERED`, `PRODUCT_VIEWED`, `CART_UPDATED`, `ORDER_CREATED`, `PAYMENT_COMPLETED`, `ORDER_PAID`) containing valid metadata and payloads.
- **`AC-003` - Bronze Ingestion Verification:** Raw event payloads are successfully ingested into Bronze storage containing raw JSON and complete Kafka transport metadata.
- **`AC-004` - Silver Transformation & Deduplication:** PySpark jobs parse Bronze data, enforce typed schemas, eliminate duplicate events, quarantine bad records, and produce clean Silver tables.
- **`AC-005` - Gold Star Schema Modeling:** Gold layer tables (`fact_orders`, `fact_order_items`, `fact_payments`, `dim_products`, `dim_customers`, `dim_date`) are accurately built and queryable.
- **`AC-006` - Airflow Automated Orchestration:** An Airflow DAG executes the Bronze $\rightarrow$ Silver $\rightarrow$ Gold pipeline on a scheduled/triggered basis without manual intervention.
- **`AC-007` - Analytical Query Demonstration:** SQL queries executed against the analytical store successfully answer all core business questions defined in Section 12.
- **`AC-008` - Zero OLTP Analytics Coupling:** Analytics queries run exclusively against the analytical store with zero direct connections to the operational PostgreSQL database.
- **`AC-009` - Local Reproducibility:** A developer can launch the entire stack via `docker compose up` and run the end-to-end verification scenario within the 16 GB RAM constraint.

---

## 16. MVP Requirements

To ensure focused delivery, requirements are explicitly partitioned into MVP and Post-MVP tiers:

### 16.1 MVP (Minimum Viable Product) Scope
- Modular monolith Spring Boot 3 backend with PostgreSQL and Redis.
- Core domain workflows: Users, Catalog, Cart, Order, Payment simulation.
- Asynchronous Kafka event publisher for core domain events.
- PySpark batch transformation pipeline implementing Bronze, Silver, and Gold tiers.
- Star Schema dimensional model supporting the core business metrics.
- Apache Airflow DAG scheduling and orchestrating pipeline runs and quality checks.
- Local analytical store (*TBD during architecture: DuckDB, ClickHouse, or BigQuery*) serving BI queries.
- Synthetic seed data script and traffic generation utility.
- Docker Compose configuration for local 1-command startup.

### 16.2 Post-MVP Scope (Future Iterations)
- **Change Data Capture (CDC):** Integration of Debezium to stream row-level database changes directly from PostgreSQL WAL logs.
- **Real-Time Streaming Aggregations:** PySpark Structured Streaming for sub-second KPI updates.
- **Real-Time Anomaly / Fraud Engine:** Continuous streaming evaluation of failed payment clusters.
- **Cloud Infrastructure Deployment:** Automated deployment to GCP (Dataproc, BigQuery, Cloud Composer) or AWS (EMR, Redshift, MWAA).

---

## 17. Requirement Traceability Matrix

| Project Brief Objective | Requirement Categories | Future Design Deliverable |
| :--- | :--- | :--- |
| **Backend E-commerce Operations** | `FR-001` - `FR-027`, `BR-001` - `BR-010` | `03-use-cases.md`, `05-database-design.md`, `08-api-specification.md` |
| **Event-Driven Decoupling** | `ER-001` - `ER-006`, `DR-006` - `DR-008` | `04-system-architecture.md`, `07-data-pipeline.md` |
| **Medallion Lakehouse Processing** | `DER-001` - `DER-012`, `DRIR-001` - `DRIR-004` | `07-data-pipeline.md` |
| **Dimensional Modeling (Star Schema)** | `DER-010`, `AR-001` - `AR-013` | `06-data-model.md` |
| **Data Quality & Reliability** | `DQR-001` - `DQR-006`, `BR-011` - `BR-014` | `06-data-model.md`, `07-data-pipeline.md` |
| **Pipeline Orchestration** | `DER-012`, `NFR-006`, `AC-006` | `07-data-pipeline.md` |
| **Local Reproducibility & Testing** | `NFR-020` - `NFR-022`, `TR-001` - `TR-008` | `04-system-architecture.md`, Implementation |
