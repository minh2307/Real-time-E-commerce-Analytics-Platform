# Analytical Data Model Specification: Real-time E-commerce Analytics Platform

## Document Metadata
- **Project Name:** Real-time E-commerce Analytics Platform
- **Document Phase:** Phase 6 — Analytical Data Model Design
- **Document Path:** `docs/06-data-model.md`
- **Upstream Dependencies:**
  - [`docs/01-project-brief.md`](file:///home/nguyen-son-minh/Downloads/Real-time%20E-commerce%20Analytics%20Platform/docs/01-project-brief.md)
  - [`docs/02-requirements.md`](file:///home/nguyen-son-minh/Downloads/Real-time%20E-commerce%20Analytics%20Platform/docs/02-requirements.md)
  - [`docs/03-use-cases.md`](file:///home/nguyen-son-minh/Downloads/Real-time%20E-commerce%20Analytics%20Platform/docs/03-use-cases.md)
  - [`docs/04-system-architecture.md`](file:///home/nguyen-son-minh/Downloads/Real-time%20E-commerce%20Analytics%20Platform/docs/04-system-architecture.md)
  - [`docs/05-database-design.md`](file:///home/nguyen-son-minh/Downloads/Real-time%20E-commerce%20Analytics%20Platform/docs/05-database-design.md)
- **Downstream Artifacts:**
  - `docs/07-data-pipeline.md` (Medallion Lakehouse, PySpark Transformations, Airflow Orchestration)
  - `docs/08-api-specification.md` (Operational & Analytical Query Endpoints)
- **Design Status:** READY FOR DATA PIPELINE DESIGN (Logically frozen; physical implementation details deferred to Phase 7)

---

## 1. Document Overview

### 1.1 Purpose
This document provides the formal **Analytical Data Model Specification** for the **Real-time E-commerce Analytics Platform**. It defines the dimensional architecture (Kimball Star Schema) governing the Gold layer of the analytical Lakehouse / Data Warehouse.

The core objective of this specification is to translate raw transactional data and domain events into clean, conformed, business-ready dimensions and atomic fact tables. It establishes mathematical boundaries for commercial revenue recognition (Gross Merchandise Value - GMV), multi-attempt payment audits, clickstream view interactions, and order lifecycle transitions.

### 1.2 Scope & Non-Goals
- **In Scope:**
  - Dimensional modeling philosophy and Kimball design patterns (conformed dimensions, atomic fact grains).
  - Detailed schema definitions for 5 conformed dimensions (`dim_date`, `dim_customers`, `dim_products`, `dim_categories`, `dim_order_status`).
  - Detailed schema definitions for 4 canonical fact tables (`fact_orders`, `fact_order_items`, `fact_payments`, `fact_product_views`).
  - Rigorous Gross Merchandise Value (GMV) semantics and boundary rules (triggered solely by `ORDER_PAID`).
  - Strict decoupling of `PAYMENT_COMPLETED` (gateway transaction audit) from `ORDER_PAID` (commercial revenue recognition).
  - Multi-attempt payment retry handling, failure analysis, and isolation from revenue recognition.
  - Guest/anonymous session clickstream modeling and customer attribution.
  - Surrogate key generation rules and natural-to-surrogate key mapping.
  - Measure classification (Additive, Semi-Additive, Non-Additive) and aggregation rules.
  - Clear distinction between order-level GMV (including tax and shipping) and product-level merchandise sales.
  - Complete semantic definitions and formulas for all business metrics (Sales, Products, Categories, Customers, Payments, Funnels).
  - Event-to-Silver-to-Gold data mapping and field-level data lineage.
  - Idempotency, deduplication mechanics, late-arriving event policies, and SCD Type 1 overwrite rules.
  - Standardized analytical SQL query implementations for core reporting use cases.
- **Out of Scope (Non-Goals):**
  - Physical application implementation code (no Java, Spring Boot, JPA entities, controllers, or repositories).
  - Physical data pipeline scripts (no PySpark jobs, Airflow DAG code, Kafka consumer/producer implementations).
  - Physical database DDL scripts, migrations, or Docker Compose configurations (already specified or deferred to implementation).
  - Redesigning upstream transactional schemas or event envelopes (which remain strictly frozen in `04-system-architecture.md` and `05-database-design.md`).
  - Devising unapproved architectural components (no CDC/Debezium infrastructure, no unsupported `ORDER_COMPLETED` domain events, no extra Gold facts).

### 1.3 Target Audience
- **Data Engineers:** To construct PySpark ingestion, cleansing, deduplication, surrogate key lookup, and dimensional loading jobs.
- **Analytics Engineers & BI Developers:** To understand table grains, measure additivity, join paths, and build dashboards (e.g., Metabase/Superset) using standard metrics.
- **Backend Engineers:** To ensure event schemas emitted by the Transactional Outbox satisfy analytical reporting contracts.
- **Technical Interviewers & Portfolio Evaluators:** To verify enterprise-grade data warehouse architecture, rigor in financial metrics (GMV vs. payment attempts), and clean decoupling between OLTP and OLAP systems.

### 1.4 Documentation Flow & Context
The project adheres to a strict documentation-first engineering progression:

```text
01 Project Brief          (Scope, dual-portfolio positioning, problem statement)
        ↓
02 Requirements           (FR, NFR, Business Rules, Data Quality, Benchmarks)
        ↓
03 Use Cases              (Customer, Admin, and Data Platform workflows)
        ↓
04 System Architecture    (Modular monolith, Outbox, Kafka, Medallion Lakehouse)
        ↓
05 Database Design        (PostgreSQL OLTP schema, constraints, Outbox table, logical storage)
        ↓
06 Data Model             [YOU ARE HERE: Analytical Star Schema, Grains, GMV Semantics, Metrics]
        ↓
07 Data Pipeline          (Lakehouse ingestion, PySpark batch jobs, Airflow DAGs, DQ enforcement)
        ↓
08 API Specification      (REST endpoints, contracts, DTOs, Swagger specifications)
        ↓
Implementation            (Production-ready codebases: Backend Java/Spring Boot & Data Spark/Airflow)
```

### 1.5 Distinction Between Database Design (05) and Data Model (06)
It is critical to distinguish between `docs/05-database-design.md` and `docs/06-data-model.md`:
- **`05-database-design.md` (Operational Focus):** Focused on third normal form (3NF) relational tables in PostgreSQL (`users`, `orders`, `order_items`, `payments`, `outbox_events`), row-level ACID transaction boundaries, atomic inventory updates, unique constraints, and physical storage indexes.
- **`06-data-model.md` (Analytical Focus):** Focuses on dimensional modeling in the Gold analytical store. It models business processes as facts and dimensions, defines explicit row grains, classifies additive/non-additive measures, formalizes financial boundaries (GMV), dictates historical price snapshotting, and specifies idempotent data transformations from the Silver layer.

---

## 2. Analytical Modeling Philosophy

### 2.1 OLTP vs. OLAP Decoupling
Operational transaction processing (OLTP) and analytical processing (OLAP) serve fundamentally opposing access patterns:
1. **OLTP (PostgreSQL 16):** Highly normalized (3NF), optimized for millisecond-latency single-row inserts and point updates, short-lived ACID transactions, and minimal write amplification. It protects operational invariants (e.g., preventing overselling of stock via atomic updates).
2. **OLAP (Lakehouse Gold Layer):** Denormalized star schema, optimized for wide columnar scans, heavy multi-table aggregations, time-series rollups, and cohort slice-and-dice operations.

**Architectural Law:** Zero analytical queries, BI reporting dashboards, or ad-hoc data science scans are permitted to touch the PostgreSQL OLTP database. All analytical reads target the Gold layer.

### 2.2 Source of Truth & Event Propagation
- **Transactional Source of Truth:** PostgreSQL is the sole transactional authority. No business entity (order, payment, user, inventory change) exists unless committed within PostgreSQL.
- **Decoupled Asynchronous Propagation:** Changes committed to PostgreSQL are captured via the Transactional Outbox pattern into `outbox_events`, polled by the Outbox Publisher, and broadcast to Kafka topics.
- **Analytical Ingestion Backbone:** Kafka acts as the high-throughput, fault-tolerant message bus decoupler. Analytical pipelines consume from Kafka topics without imposing any query load or contention on the operational database.

### 2.3 Medallion Lakehouse Paradigm (Bronze, Silver, Gold)
Data flows through three distinct functional tiers:
1. **Bronze Layer (Raw Ingestion):** Append-only, immutable Parquet archive of every raw Kafka event message. Preserves exact payloads, ingestion timestamps, Kafka partitions, and offsets. Enables complete historical replayability and backfills.
2. **Silver Layer (Cleaned & Conformed):** Typed, normalized, schema-validated, and deduplicated datasets. Enforces data quality gates, filters corrupted messages, routes malformed records to quarantine, and consolidates operational change events into clean entity logs.
3. **Gold Layer (Dimensional Star Schema):** Business-ready dimensional models (Kimball methodology). Joins dimensions and facts via compact surrogate keys, enforces business boundary semantics (such as GMV), snapshots historical price data, and serves high-performance analytics.

### 2.4 Dimensional Modeling Core Tenets (Kimball Methodology)
The Gold analytical layer is designed strictly according to Ralph Kimball’s dimensional modeling framework:
- **Conformed Dimensions:** Shared across business processes to enable cross-functional drill-down (e.g., analyzing orders and product views by the same `dim_date` and `dim_products`).
- **Atomic Facts:** Built at the lowest atomic grain of operational capture (individual order line item, discrete payment attempt, single product view event). Atomic facts provide maximum analytical flexibility without loss of fidelity.
- **Surrogate Keys:** Artificial integer keys decouple analytical structures from operational database primary keys, insulate against operational schema refactors, and boost join performance.

### 2.5 Grain Discipline & Invariant Preservation
Every fact table must declare an unambiguous, mathematically precise **Grain**. The grain dictates the real-world operational event represented by a single row. Violations of grain discipline lead to fatal data warehouse defects (fan-out traps, duplicate metric counting, corrupted financial reporting).

### 2.6 Idempotency, Replayability, and Backfill Determinism
Data pipelines inevitably encounter failures, network partitions, and reprocessing requirements. Therefore, the data model guarantees:
- **Deterministic Reprocessing:** Running a transformation job over a historical time slice (e.g., day `YYYY-MM-DD`) multiple times produces the exact same Gold output.
- **Natural Key Upsert/Overwrite:** Partitions and records in Gold are merged or overwritten using deterministic business keys (`order_id`, `payment_id`, `event_id`), preventing duplicate rows during Airflow retries or backfills.

---

## 3. Data Modeling Layers (Medallion Flow)

```text
  +-------------------------------------------------------------+
  |                   PostgreSQL OLTP Database                  |
  | (users, categories, products, orders, payments, outbox_events)|
  +-------------------------------------------------------------+
                                 │
                     Transactional Outbox Poll
                                 │
                                 ▼
  +-------------------------------------------------------------+
  |                        Apache Kafka                         |
  |  Topics: ecommerce.orders, ecommerce.payments, views, users  |
  +-------------------------------------------------------------+
                                 │
                     Batch / Micro-batch Stream
                                 │
                                 ▼
  +-------------------------------------------------------------+
  |                     BRONZE LAYER (RAW)                      |
  | - Append-only Parquet storage partitioned by ingestion_date |
  | - Retains full raw payload, headers, kafka_offset, event_id |
  | - Immutable & Replayable audit history                      |
  +-------------------------------------------------------------+
                                 │
                     PySpark Cleaning & Validation
                                 │
                                 ▼
  +-------------------------------------------------------------+
  |                    SILVER LAYER (CLEANSED)                  |
  | - Parsed JSON schemas into strongly typed columns           |
  | - Deduplicated on event_id via window functions             |
  | - Filtered invalid records (quarantine for malformed data)  |
  | - Normalized timestamps into UTC TIMESTAMPTZ                |
  | - Silver Cart helper/staging datasets for funnel analysis   |
  +-------------------------------------------------------------+
                                 │
                     Dimensional Modeling & GMV Logic
                                 │
                                 ▼
  +-------------------------------------------------------------+
  |                    GOLD LAYER (STAR SCHEMA)                 |
  | Conformed Dimensions:                                       |
  |   dim_date, dim_customers, dim_products,                    |
  |   dim_categories, dim_order_status                          |
  |                                                             |
  | 4 Canonical Fact Tables:                                    |
  |   fact_orders (Grain: 1 row / order, GMV boundary)          |
  |   fact_order_items (Grain: 1 row / line item, price snapshot)|
  |   fact_payments (Grain: 1 row / payment attempt)            |
  |   fact_product_views (Grain: 1 row / clickstream view)      |
  +-------------------------------------------------------------+
```

### 3.1 Bronze Layer: Raw Event Storage
- **Format:** Snappy-compressed Apache Parquet files.
- **Partitioning:** Partitioned by ingestion date: `/bronze/{topic}/year=YYYY/month=MM/day=DD/`.
- **Characteristics:**
  - Raw JSON string payloads stored in a `raw_payload` column.
  - Metadata enriched: `event_id`, `kafka_topic`, `kafka_partition`, `kafka_offset`, `kafka_timestamp`, `ingestion_timestamp`.
  - Append-only: No updates or deletions are ever executed in Bronze.
  - Replay Guarantee: Dropping downstream Silver and Gold tables allows complete historical reconstitution from Bronze.

### 3.2 Silver Layer: Cleaned, Normalized, and Deduplicated
- **Format:** Snappy-compressed Apache Parquet tables.
- **Partitioning:** Partitioned by business event date: `/silver/{entity}/event_date=YYYY-MM-DD/`.
- **Characteristics:**
  - Strong Typing: JSON strings unpacked into typed primitives (UUID, TIMESTAMP, DECIMAL, INTEGER, BOOLEAN).
  - Deduplication: Ingested events deduplicated by `event_id` using `ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingestion_timestamp ASC)`.
  - Schema Conformance: Enforces schema evolution rules (backward compatible JSON schema v1.0).
  - Malformed Routing: Corrupted payloads (e.g., unparseable JSON, missing required headers) are redirected to a dedicated Quarantine storage table (`quarantine_events`) with error classification metadata.
  - Analytical Helper Datasets: Silver hosts cart helper datasets (`silver_cart_items`, `silver_cart_events`) used exclusively for funnel drop-off analytics. These datasets are staging/helper assets and do not form part of the canonical Gold fact tables.

### 3.3 Gold Layer: Business-Ready Dimensional Marts
- **Format:** Snappy-compressed Apache Parquet or specialized columnar Analytical Store (DuckDB / ClickHouse).
- **Structure:** Kimball Star Schema consisting of 4 centralized canonical Fact tables linked to 5 shared Conformed Dimensions via surrogate keys.
- **Characteristics:**
  - Business Semantics Applied: Computes metrics such as GMV strictly from `ORDER_PAID`, snapshots historical item prices at order placement time, resolves surrogate foreign keys, and evaluates order lifecycle status.
  - Atomic Overwrite / Merge: Data loads are partition-scoped and idempotent on primary business keys (`order_id`, `payment_id`, `event_id`).
  - Optimized for Queries: Columnar layout with aggressive dictionary encoding, run-length compression, and metadata min/max statistics for ultra-fast vector scans.

---

## 4. Conformed Dimensions Architecture

### 4.1 Definition & Role of Conformed Dimensions
A **Conformed Dimension** is an analytical dimension that has the exact same meaning, primary key, attribute names, and domain values across every fact table to which it connects. 

In this platform, conformed dimensions are the linchpin that allows cross-functional analytics. For example, an analyst can query `dim_products` and seamlessly join both `fact_order_items` (measuring purchases) and `fact_product_views` (measuring shopper interest) without creating siloed, mismatched data definitions.

### 4.2 Core Conformed Dimensions Matrix

| Dimension Name | Natural Business Key | Surrogate Key | Supporting Business Entities | Referenced By Fact Tables |
| :--- | :--- | :--- | :--- | :--- |
| `dim_date` | Calendar Date (`DATE`) | `date_key` (`INT`) | Calendar Temporal Model | `fact_orders`, `fact_order_items`, `fact_payments`, `fact_product_views` |
| `dim_customers` | `users.id` (`UUID`) | `customer_key` (`BIGINT`) | Registered Customer Accounts | `fact_orders`, `fact_payments`, `fact_product_views` (nullable) |
| `dim_products` | `products.id` (`UUID`) | `product_key` (`BIGINT`) | Product Catalog Items | `fact_order_items`, `fact_product_views` |
| `dim_categories` | `categories.id` (`UUID`) | `category_key` (`BIGINT`) | Catalog Hierarchy Classifications | `dim_products` (Outrigger / Direct reference) |
| `dim_order_status` | Status Code (`VARCHAR`) | `status_key` (`INT`) | Finite Order State Machine | `fact_orders` |

### 4.3 Dimensional Reusability Across Business Processes
By conforming dimensions across facts, the platform enables single-query analytical correlation across distinct customer interactions:
- **Conversion Funnel Correlation:** Linking `dim_products` and `dim_date` across `fact_product_views` and `fact_order_items` yields Product Conversion Ratios without data duplication.
- **Customer Lifetime Value (LTV):** Aggregating `fact_orders` across `dim_customers` computes historical customer spend, average order value, and repeat purchase frequency.
- **Payment Success Analytics:** Aggregating `fact_payments` across `dim_customers` and `dim_date` highlights customer-level payment failure trends and gateway reliability.

### 4.4 Optional & Future Extensions
The MVP explicitly restricts Gold dimensions to the 5 conformed dimensions above. Future architectural extensions may introduce:
- `dim_payment_methods`: Card brand, issuing bank, gateway token type.
- `dim_geography`: Shipping destination country, state, city, postal zone.
- `dim_promotions`: Discount vouchers, coupon codes, campaign identifiers.
These future extensions will connect directly to existing fact tables without requiring modifications to the core fact grains.

---

## 5. Detailed Dimension Models

### 5.1 `dim_date`

#### 5.1.1 Purpose & Grain
- **Purpose:** Provides a centralized, conformed calendar dimension supporting uniform temporal slicing, rollups, and time-series analysis across all business processes (orders, items, payments, clickstream).
- **Grain:** Exactly **one row per calendar day**.

#### 5.1.2 Schema Specification

| Column Name | Physical Data Type | Nullable | Key Type | Description / Domain Constraints | Example Value |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `date_key` | `INTEGER` | `NOT NULL` | **PK** | Surrogate integer key in `YYYYMMDD` format | `20260925` |
| `calendar_date` | `DATE` | `NOT NULL` | **NK** | Standard ISO-8601 calendar date (`YYYY-MM-DD`) | `2026-09-25` |
| `day_of_month` | `SMALLINT` | `NOT NULL` | Attribute | Numeric day of the month (`1` to `31`) | `25` |
| `day_of_week` | `SMALLINT` | `NOT NULL` | Attribute | Day index (`1` = Sunday, `2` = Monday, ..., `7` = Saturday) | `6` |
| `day_name` | `VARCHAR(15)` | `NOT NULL` | Attribute | Full English name of the day | `'Friday'` |
| `day_name_short`| `VARCHAR(3)` | `NOT NULL` | Attribute | Abbreviated day name | `'Fri'` |
| `is_weekend` | `BOOLEAN` | `NOT NULL` | Attribute | `TRUE` if day is Saturday or Sunday, else `FALSE` | `FALSE` |
| `week_of_year` | `SMALLINT` | `NOT NULL` | Attribute | ISO calendar week number (`1` to `53`) | `39` |
| `month_number` | `SMALLINT` | `NOT NULL` | Attribute | Calendar month index (`1` to `12`) | `9` |
| `month_name` | `VARCHAR(15)` | `NOT NULL` | Attribute | Full English month name | `'September'` |
| `month_name_short`| `VARCHAR(3)`| `NOT NULL` | Attribute | Abbreviated month name | `'Sep'` |
| `quarter_number`| `SMALLINT` | `NOT NULL` | Attribute | Calendar quarter (`1` to `4`) | `3` |
| `quarter_name` | `VARCHAR(2)` | `NOT NULL` | Attribute | Calendar quarter label (`'Q1'`, `'Q2'`, `'Q3'`, `'Q4'`) | `'Q3'` |
| `year_number` | `SMALLINT` | `NOT NULL` | Attribute | Four-digit calendar year | `2026` |
| `year_month` | `VARCHAR(7)` | `NOT NULL` | Attribute | Monthly rollup string (`YYYY-MM`) | `'2026-09'` |
| `is_holiday` | `BOOLEAN` | `NOT NULL` | Attribute | Default `FALSE` (extensible for national promotional calendars) | `FALSE` |

#### 5.1.3 Static Pre-Population Rationale
- **Temporal Horizon:** Fully pre-populated for calendar years **2024 through 2030** (2,557 total rows).
- **Architectural Rationale:**
  - **Zero ETL Overhead:** Date dimensions represent a known, fixed mathematical continuum. Generating dates dynamically during PySpark ETL runs wastes compute resources and introduces race conditions.
  - **Fast Integer Joins:** Analytical queries join on integer surrogate keys (`fact_orders.order_date_key = dim_date.date_key`), avoiding expensive string or timestamp truncation operations during query execution.
  - **Reporting Uniformity:** All analytical reporting tools (BI dashboards, SQL queries) share identical calendar logic (e.g., standardized start-of-week and quarter boundaries).

---

### 5.2 `dim_customers`

#### 5.2.1 Purpose & Grain
- **Purpose:** Represents customer profile entities, enabling customer-level segmentation, retention analysis, lifetime value calculations, and repeat buyer tracking.
- **Grain:** Exactly **one row per registered customer**.

#### 5.2.2 Schema Specification

| Column Name | Physical Data Type | Nullable | Key Type | Description / Domain Constraints | Example Value |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `customer_key` | `BIGINT` | `NOT NULL` | **PK** | Monotonically increasing surrogate key | `10042` |
| `customer_id` | `UUID` | `NOT NULL` | **NK** | Operational user primary key from PostgreSQL `users.id` | `'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'` |
| `email` | `VARCHAR(255)` | `NOT NULL` | Attribute | Customer email address (used for contactability analysis) | `'customer@example.com'` |
| `username` | `VARCHAR(100)` | `NOT NULL` | Attribute | Operational username | `'johndoe'` |
| `full_name` | `VARCHAR(255)` | `NOT NULL` | Attribute | Customer full display name | `'John Doe'` |
| `role` | `VARCHAR(50)` | `NOT NULL` | Attribute | Role designation (`'CUSTOMER'`, `'ADMIN'`) | `'CUSTOMER'` |
| `status` | `VARCHAR(50)` | `NOT NULL` | Attribute | Account status (`'ACTIVE'`, `'LOCKED'`, `'DEACTIVATED'`) | `'ACTIVE'` |
| `registration_date_key` | `INTEGER` | `NOT NULL` | **FK** | Foreign key linking to `dim_date.date_key` | `20260901` |
| `created_at` | `TIMESTAMPTZ` | `NOT NULL` | Audit | Account creation timestamp in UTC | `2026-09-01T10:00:00Z` |
| `updated_at` | `TIMESTAMPTZ` | `NOT NULL` | Audit | Last modification timestamp in UTC | `2026-09-20T14:30:00Z` |

#### 5.2.3 SCD Type 1 Mechanics & Lineage
- **SCD Policy:** Slowly Changing Dimension Type 1 (**In-Place Overwrite**).
- **Lineage Flow:**
  ```text
  PostgreSQL users (id)
         ↓
  Transactional Outbox (USER_REGISTERED)
         ↓
  Kafka Topic ecommerce.users
         ↓
  Bronze bronze_users (raw JSON)
         ↓
  Silver silver_customers (deduplicated on users.id)
         ↓
  Gold dim_customers (keyed by customer_key, matching on customer_id)
  ```
- **Type 1 Rationale:** Customer attribute updates (e.g., updating `full_name` or `status`) overwrite existing dimensional attributes in place. The platform does not require historical point-in-time reconstruction of customer names; this maintains simplicity and eliminates dimensional explosion.
- **Anonymous Customer Fallback:** A dedicated system record (`customer_key = -1`, `customer_id = '00000000-0000-0000-0000-000000000000'`, `full_name = 'Anonymous Guest'`) is seeded to support fact joins for guest clickstream events.

---

### 5.3 `dim_products`

#### 5.3.1 Purpose & Grain
- **Purpose:** Represents current catalog merchandise available in the e-commerce store, supporting product performance rankings, inventory analysis, and category rollups.
- **Grain:** Exactly **one row per product catalog item**.

#### 5.3.2 Schema Specification

| Column Name | Physical Data Type | Nullable | Key Type | Description / Domain Constraints | Example Value |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `product_key` | `BIGINT` | `NOT NULL` | **PK** | Monotonically increasing surrogate key | `20015` |
| `product_id` | `UUID` | `NOT NULL` | **NK** | Operational product primary key from PostgreSQL `products.id` | `'b1eebc99-9c0b-4ef8-bb6d-6bb9bd380b22'` |
| `sku` | `VARCHAR(100)` | `NOT NULL` | Attribute | Stock Keeping Unit code | `'TECH-KEY-001'` |
| `title` | `VARCHAR(255)` | `NOT NULL` | Attribute | Current active product display title | `'Mechanical Wireless Keyboard'` |
| `description` | `TEXT` | `NULL` | Attribute | Current active product description | `'RGB Backlit, Brown Switches'` |
| `category_key` | `BIGINT` | `NOT NULL` | **FK** | Foreign key linking to `dim_categories.category_key` | `5001` |
| `category_id` | `UUID` | `NOT NULL` | Attribute | Operational category ID from PostgreSQL `categories.id` | `'c2eebc99-9c0b-4ef8-bb6d-6bb9bd380c33'` |
| `current_price`| `NUMERIC(12,2)`| `NOT NULL` | Attribute | Current active catalog price (`>= 0.00`) | `129.99` |
| `is_active` | `BOOLEAN` | `NOT NULL` | Attribute | Current catalog listing status (`TRUE`/`FALSE`) | `TRUE` |
| `created_at` | `TIMESTAMPTZ` | `NOT NULL` | Audit | Catalog record creation timestamp in UTC | `2026-08-15T08:00:00Z` |
| `updated_at` | `TIMESTAMPTZ` | `NOT NULL` | Audit | Last catalog modification timestamp in UTC | `2026-09-24T18:00:00Z` |

#### 5.3.3 Catalog Attributes vs. Historical Line Item Snapshots
- **Critical Architectural Invariant:** `dim_products.current_price` represents the *current listing price* in the active store catalog.
- **Strict Accounting Rule:** Under no circumstances should `dim_products.current_price` be joined with historical orders to compute past sales revenue or GMV! 
  If a merchant changes a product's price from \$100 to \$150 today, joining with `dim_products.current_price` would falsely alter historical revenue for orders placed last month.
- **Historical Price Protection:** Historical unit prices, product titles, and SKUs are immutably snapshotted directly inside `fact_order_items` at the moment of checkout from the order creation payload. `dim_products` is strictly used for slicing current catalog availability and current category assignments.

---

### 5.4 `dim_categories`

#### 5.4.1 Purpose & Grain
- **Purpose:** Represents product classification taxonomy, enabling category-level revenue rollups, merchandise mix analysis, and browsing taxonomy navigation.
- **Grain:** Exactly **one row per product category**.

#### 5.4.2 Schema Specification

| Column Name | Physical Data Type | Nullable | Key Type | Description / Domain Constraints | Example Value |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `category_key` | `BIGINT` | `NOT NULL` | **PK** | Monotonically increasing surrogate key | `5001` |
| `category_id` | `UUID` | `NOT NULL` | **NK** | Operational category primary key from PostgreSQL `categories.id` | `'c2eebc99-9c0b-4ef8-bb6d-6bb9bd380c33'` |
| `name` | `VARCHAR(100)` | `NOT NULL` | Attribute | Category display name | `'Keyboards & Mice'` |
| `slug` | `VARCHAR(120)` | `NOT NULL` | Attribute | Unique URL slug identifier | `'keyboards-mice'` |
| `parent_category_id` | `UUID` | `NULL` | Attribute | Operational ID of parent category (self-referential) | `'d3eebc99-9c0b-4ef8-bb6d-6bb9bd380d44'` |
| `description` | `TEXT` | `NULL` | Attribute | Category description text | `'Computer input peripherals'` |
| `created_at` | `TIMESTAMPTZ` | `NOT NULL` | Audit | Category creation timestamp in UTC | `2026-08-01T00:00:00Z` |
| `updated_at` | `TIMESTAMPTZ` | `NOT NULL` | Audit | Last category update timestamp in UTC | `2026-08-01T00:00:00Z` |

#### 5.4.3 Hierarchy & SCD Type 1 Policy
- **No Complex Snowflaking:** For the MVP, categories remain a simple outrigger linked to `dim_products`. Deep recursive snowflaking is avoided to preserve simple single-hop joins in analytical queries.
- **SCD Policy:** SCD Type 1 (In-Place Overwrite). Updates to category names or descriptions overwrite the existing record in `dim_categories`.
- **Category Reorganization Limitation:** If a product is reassigned from Category A to Category B, historical sales volume for that product will roll up into Category B under SCD Type 1 queries. Historical category attribution is intentionally subject to SCD Type 1 behavior; only monetary and line quantities are protected via fact snapshots.

---

### 5.5 `dim_order_status`

#### 5.5.1 Purpose & Grain
- **Purpose:** Decouples order status lifecycle strings from fact rows, formalizes the finite state machine into analytical dimensions, and provides standardized classification flags for reporting queries.
- **Grain:** Exactly **one row per valid order status** (5 static rows).

#### 5.5.2 Schema Specification

| Column Name | Physical Data Type | Nullable | Key Type | Description / Domain Constraints | Example Value |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `status_key` | `INTEGER` | `NOT NULL` | **PK** | Static surrogate identifier (`1` to `5`) | `2` |
| `status_code` | `VARCHAR(50)` | `NOT NULL` | **NK** | Operational status string from PostgreSQL `orders.status` | `'PAID'` |
| `display_name` | `VARCHAR(50)` | `NOT NULL` | Attribute | Human-readable UI display label | `'Payment Confirmed'` |
| `is_terminal` | `BOOLEAN` | `NOT NULL` | Attribute | `TRUE` if state cannot transition further | `FALSE` |
| `is_paid_eligible` | `BOOLEAN` | `NOT NULL` | Attribute | `TRUE` if order has successfully paid (eligible for GMV) | `TRUE` |
| `description` | `TEXT` | `NOT NULL` | Attribute | Business definition of this lifecycle stage | `'Order successfully paid and awaiting shipment'` |

#### 5.5.3 Complete Domain Enumeration

```sql
-- Static Population for dim_order_status
-- Key 1: PENDING        | Terminal: FALSE | Paid Eligible: FALSE | Awaiting payment attempt
-- Key 2: PAID           | Terminal: FALSE | Paid Eligible: TRUE  | Payment succeeded, GMV recognized
-- Key 3: PAYMENT_FAILED | Terminal: FALSE | Paid Eligible: FALSE | Payment rejected, awaiting customer retry
-- Key 4: CANCELLED      | Terminal: TRUE  | Paid Eligible: FALSE | Order terminated, inventory returned
-- Key 5: COMPLETED      | Terminal: TRUE  | Paid Eligible: TRUE  | Order delivered, customer fulfillment complete
```

#### 5.5.4 Rationale for Status Dimension
- **Degenerate String Elimination:** Replaces repetitive strings across millions of order rows with a compact 4-byte integer foreign key.
- **Instant Boolean Filtering:** Enables dashboard queries to filter paid orders using `WHERE status.is_paid_eligible = TRUE` without hardcoding status strings across disparate BI dashboards.

---

## 6. Fact Models & Explicit Grains

### 6.1 Grain Architecture & Foundation
In dimensional modeling, **Grain** is the fundamental contract defining what a single physical row in a fact table represents. An ambiguous or shifting grain breaks relational integrity, produces wildly inflated metrics during multi-table joins, and invalidates financial statements.

### 6.2 Mathematical Risks of Ambiguous Grain
- **The 1-to-Many Fan-out Trap:** If an analyst joins `fact_orders` directly with `fact_order_items` on `order_id` without pre-aggregating, an order with 5 line items will duplicate the `fact_orders.total_amount` measure 5 times. Summing `total_amount` on this joined result multiplies reported revenue by 500%!
- **Multi-Attempt Payment Distortion:** If an order underwent 3 failed payment attempts before succeeding, joining `fact_orders` to `fact_payments` without strict grain control causes order-level measures to appear 4 times.
- **Grain Enforcement:** To completely eliminate fan-out traps, each of the four fact tables is strictly segregated by atomic grain.

### 6.3 Core Fact Matrix

| Fact Table Name | Real-World Business Process | Declared Atomic Grain | Primary Natural Key | Primary Surrogate Key |
| :--- | :--- | :--- | :--- | :--- |
| `fact_orders` | Order placement, payment transition, and fulfillment lifecycle | **One row per discrete commercial order** | `order_id` (`UUID`) | `order_key` (`BIGINT`) |
| `fact_order_items` | Individual product sales within an order | **One row per line item inside an order** | `order_item_id` (`UUID`) | `order_item_key` (`BIGINT`) |
| `fact_payments` | Discrete financial gateway transaction attempts | **One row per payment gateway attempt** | `payment_id` (`UUID`) | `payment_key` (`BIGINT`) |
| `fact_product_views` | Shopper clickstream product page interactions | **One row per product view interaction event** | `event_id` (`UUID`) | `view_key` (`BIGINT`) |

---

## 7. Detailed Fact Table Specifications

### 7.1 `fact_orders`

#### 7.1.1 Declared Grain
**Exactly one row per customer order entity** (`orders.id`).

#### 7.1.2 Schema Specification

| Column Name | Physical Data Type | Nullable | Key Type | Description / Business Invariants |
| :--- | :--- | :--- | :--- | :--- |
| `order_key` | `BIGINT` | `NOT NULL` | **PK** | Monotonically increasing surrogate key |
| `order_id` | `UUID` | `NOT NULL` | **NK** | Operational order UUID from PostgreSQL `orders.id` |
| `customer_key` | `BIGINT` | `NOT NULL` | **FK** | Foreign key linking to `dim_customers.customer_key` |
| `order_date_key` | `INTEGER` | `NOT NULL` | **FK** | Foreign key linking to `dim_date.date_key` (order creation date) |
| `paid_date_key` | `INTEGER` | `NULL` | **FK** | Foreign key linking to `dim_date.date_key` (populated ONLY on `ORDER_PAID`) |
| `status_key` | `INTEGER` | `NOT NULL` | **FK** | Foreign key linking to `dim_order_status.status_key` |
| `subtotal_amount`| `NUMERIC(12,2)`| `NOT NULL` | Measure | Sum of item lines (`>= 0.00`) |
| `tax_amount` | `NUMERIC(12,2)`| `NOT NULL` | Measure | Calculated sales tax (`>= 0.00`) |
| `shipping_amount`| `NUMERIC(12,2)`| `NOT NULL` | Measure | Freight shipping fee (`>= 0.00`) |
| `total_amount` | `NUMERIC(12,2)`| `NOT NULL` | Measure | Invariant: `subtotal + tax + shipping` |
| `gmv_amount` | `NUMERIC(12,2)`| `NOT NULL` | Measure | **Gross Merchandise Value:** Recognized solely upon `ORDER_PAID`, else `0.00` |
| `is_paid` | `BOOLEAN` | `NOT NULL` | Flag | `TRUE` if `ORDER_PAID` event was received, else `FALSE` |
| `created_at` | `TIMESTAMPTZ` | `NOT NULL` | Audit | Order creation timestamp in UTC |
| `updated_at` | `TIMESTAMPTZ` | `NOT NULL` | Audit | Last operational update timestamp in UTC |
| `cancelled_at` | `TIMESTAMPTZ` | `NULL` | Temporal | Timestamp when order was cancelled (`NULL` if active/completed) |
| `completed_at` | `TIMESTAMPTZ` | `NULL` | Temporal | Timestamp when order was fulfilled (`NULL` if unfulfilled) |

---

### 7.2 Gross Merchandise Value (GMV) Semantics

#### 7.2.1 Authoritative GMV Trigger & Fundamental Rules
- **The Core Rule:** Gross Merchandise Value (GMV) is recognized **if and only if** a valid **`ORDER_PAID`** event is processed by the pipeline.
- **Formal Invariant:**
  ```text
  ORDER_PAID is the authoritative commercial event for GMV recognition.

  PAID and COMPLETED are order states.
  They are not independent GMV recognition events.

  A replay, duplicate ORDER_PAID, PAYMENT_COMPLETED,
  or later COMPLETED transition must never create additional GMV.
  ```

#### 7.2.2 State Transition vs. GMV Recognition
When `ORDER_PAID` arrives:
```text
is_paid = TRUE
gmv_amount = total_amount
paid_date_key = date(event_timestamp)
status_key = 2 (PAID)
```
- **Order State `COMPLETED`:** Represents order fulfillment. It simply inherits the previously recognized paid status (`is_paid = TRUE`, `gmv_amount = total_amount`). `COMPLETED` is an order state, NOT an independent GMV recognition event. An order reaching `COMPLETED` without a prior `ORDER_PAID` event must never independently synthesize GMV.

#### 7.2.3 Decoupling `PAYMENT_COMPLETED` from GMV Recognition
In many naive systems, analysts incorrectly treat `PAYMENT_COMPLETED` events as the signal to recognize revenue. This platform enforces strict decoupling:
1. **Transaction Audit vs. Commercial Order Contract:**
   - `PAYMENT_COMPLETED` is an infrastructure audit event emitted by the payment gateway module. It asserts: *"The banking gateway successfully captured \$X for attempt Y."* It is stored in `fact_payments`.
   - `ORDER_PAID` is a commercial business event emitted by the order domain. It asserts: *"The order has received confirmed payment, inventory allocation is finalized, and commercial revenue is legally recognized."* It updates `fact_orders`.
2. **Multi-Attempt Payment Retries (Zero Double-Counting):**
   - Scenario: A customer places Order `#ORD-1` for \$100.
   - Attempt 1 fails (network timeout). The system records `PAYMENT_FAILED` in `fact_payments` (`attempt_number = 1`, `status = 'FAILED'`). Order status remains `PAYMENT_FAILED`. GMV is \$0.00.
   - Customer clicks retry. Attempt 2 succeeds. The payment module emits `PAYMENT_COMPLETED`, recorded in `fact_payments` (`attempt_number = 2`, `status = 'SUCCESS'`).
   - The ordering engine processes the confirmed payment, transitions order status from `PAYMENT_FAILED` to `PAID`, and emits a single `ORDER_PAID` event.
   - The pipeline ingests `ORDER_PAID`, updates `fact_orders` (`is_paid = TRUE`, `gmv_amount = 100.00`, `paid_date_key = 20260925`).
   - **Audit vs. Revenue Result:** `fact_payments` contains **2 physical rows** (complete financial audit of attempts), while `fact_orders` recognizes GMV **exactly once for \$100.00**.
3. **Replay & Webhook Duplication Protection:**
   - If payment webhooks are delivered multiple times by an external provider, or if the Kafka topic is replayed from offset 0, repeated `PAYMENT_COMPLETED` events will re-enter the pipeline.
   - Because `fact_orders` is keyed by `order_id` and recognizes GMV solely from `ORDER_PAID`, repeated payment events can never inflate GMV.

#### 7.2.4 Order Cancellation Impact
- Orders cancelled prior to payment (`PENDING` $\rightarrow$ `CANCELLED` or `PAYMENT_FAILED` $\rightarrow$ `CANCELLED`) have `is_paid = FALSE` and `gmv_amount = 0.00`. They contribute zero revenue.
- If an order is cancelled, `fact_orders.status_key` points to `CANCELLED`, and `fact_orders.cancelled_at` is populated.

---

### 7.3 Order Lifecycle Modeling in Analytical Marts

#### 7.3.1 Finite State Machine Alignment
The analytical data model reflects the operational state machine:

```text
    +-------------------------------------------------------+
    |                        PENDING                        |
    +-------------------------------------------------------+
           │                     │                     │
      Payment OK           Payment Fail            Customer
           │                     │                  Cancel
           ▼                     ▼                     │
    +-------------+      +----------------+            │
    |    PAID     |      | PAYMENT_FAILED |            │
    +-------------+      +----------------+            │
           │               │            │              │
        Deliver        Retry OK      Customer          │
           │               │          Cancel           │
           ▼               │            │              │
    +-------------+        │            ▼              │
    |  COMPLETED  |◄───────┴─────►+-----------+        │
    |  (Terminal) |               | CANCELLED |◄───────┘
    +-------------+               | (Terminal)|
                                  +-----------+
```

#### 7.3.2 In-Place State Transitions in `fact_orders`
`fact_orders` maintains exactly **one physical row per `order_id`**. Operational lifecycle events update this single row in place via idempotent merge/overwrite:
1. `ORDER_CREATED`: Inserts initial row into `fact_orders`. `status_key` = `1 (PENDING)`, `is_paid` = `FALSE`, `gmv_amount` = `0.00`, `order_date_key` = `DATE(created_at)`.
2. `PAYMENT_FAILED`: Updates existing order row. `status_key` = `3 (PAYMENT_FAILED)`, `is_paid` = `FALSE`, `gmv_amount` = `0.00`.
3. `ORDER_PAID`: Updates existing order row. `status_key` = `2 (PAID)`, `paid_date_key` = `DATE(event_timestamp)`, `is_paid` = `TRUE`, `gmv_amount` = `total_amount`.
4. `ORDER_CANCELLED`: Updates existing order row. `status_key` = `4 (CANCELLED)`, `cancelled_at` = `event_timestamp`. GMV remains `0.00`.
5. Order Fulfillment: Updates existing order row. `status_key` = `5 (COMPLETED)`, `completed_at` = `event_timestamp`. Inherits `is_paid = TRUE` and `gmv_amount`.

---

### 7.4 `fact_order_items`

#### 7.4.1 Declared Grain
**Exactly one row per line item within a customer order** (`order_items.id`).

#### 7.4.2 Schema Specification

| Column Name | Physical Data Type | Nullable | Key Type | Description / Business Invariants |
| :--- | :--- | :--- | :--- | :--- |
| `order_item_key` | `BIGINT` | `NOT NULL` | **PK** | Monotonically increasing surrogate key |
| `order_key` | `BIGINT` | `NOT NULL` | **FK** | Foreign key linking to parent `fact_orders.order_key` |
| `product_key` | `BIGINT` | `NOT NULL` | **FK** | Foreign key linking to `dim_products.product_key` |
| `order_date_key` | `INTEGER` | `NOT NULL` | **FK** | Foreign key linking to `dim_date.date_key` |
| `product_sku_snapshot` | `VARCHAR(100)`| `NOT NULL` | Snapshot | Immutable SKU string captured at order creation |
| `product_title_snapshot`| `VARCHAR(255)`| `NOT NULL`| Snapshot | Immutable product title captured at order creation |
| `unit_price_snapshot` | `NUMERIC(12,2)`| `NOT NULL` | Snapshot | Immutable unit price captured at order creation (`>= 0.00`) |
| `quantity` | `INTEGER` | `NOT NULL` | Measure | Quantity purchased (`quantity > 0`) |
| `line_total_amount` | `NUMERIC(12,2)`| `NOT NULL` | Measure | Invariant: `unit_price_snapshot * quantity` (Merchandise Line Sales) |

#### 7.4.3 Historical Price Snapshotting Rationale & Rule
- **The Accounting Rule:**
  ```text
  Historical order item attributes must originate from the
  order transaction snapshot captured at order creation.

  Current product dimension attributes must never overwrite
  historical order item price/title/SKU snapshots.
  ```
- **The Invariant:** Even if a merchant updates a product's price from \$100 to \$150 in `dim_products` (via SCD Type 1 overwrite), every historical line item in `fact_order_items` permanently retains its original legally binding unit price (`unit_price_snapshot`) and `line_total_amount`.

---

### 7.5 `fact_payments`

#### 7.5.1 Declared Grain
**Exactly one row per discrete payment gateway attempt** (`payments.id`).

#### 7.5.2 Schema Specification

| Column Name | Physical Data Type | Nullable | Key Type | Description / Business Invariants |
| :--- | :--- | :--- | :--- | :--- |
| `payment_key` | `BIGINT` | `NOT NULL` | **PK** | Monotonically increasing surrogate key |
| `payment_id` | `UUID` | `NOT NULL` | **NK** | Operational payment UUID from PostgreSQL `payments.id` |
| `order_key` | `BIGINT` | `NOT NULL` | **FK** | Foreign key linking to target `fact_orders.order_key` |
| `customer_key` | `BIGINT` | `NOT NULL` | **FK** | Foreign key linking to initiating `dim_customers.customer_key` |
| `payment_date_key`| `INTEGER` | `NOT NULL` | **FK** | Foreign key linking to `dim_date.date_key` |
| `attempt_number` | `INTEGER` | `NOT NULL` | Attribute | Sequential attempt counter for order (`1`, `2`, `3`...) |
| `status` | `VARCHAR(50)` | `NOT NULL` | Attribute | Gateway outcome (`'SUCCESS'`, `'FAILED'`) |
| `payment_amount` | `NUMERIC(12,2)`| `NOT NULL` | Measure | Transaction amount submitted to gateway (`>= 0.00`) |
| `currency` | `VARCHAR(10)` | `NOT NULL` | Attribute | ISO currency code (fixed `'USD'` for MVP) |
| `payment_method` | `VARCHAR(50)` | `NOT NULL` | Attribute | Method identifier (`'CREDIT_CARD'`) |
| `error_code` | `VARCHAR(50)` | `NULL` | Attribute | Failure reason code (`'INSUFFICIENT_FUNDS'`, `'CARD_EXPIRED'`) |
| `created_at` | `TIMESTAMPTZ` | `NOT NULL` | Audit | Timestamp when payment attempt was executed in UTC |

#### 7.5.3 Append-Only Audit Integrity & Retry Semantics
- **No Overwriting:** Payment attempts are **strictly append-only**. If an order fails on attempt 1 and succeeds on attempt 2, `fact_payments` contains **two distinct physical rows**.
- **Financial Audit Trail:** Storing every attempt enables precise calculation of payment gateway authorization rates, error code frequency, and customer retry behavior without distorting commercial order counts.

---

### 7.6 `fact_product_views`

#### 7.6.1 Declared Grain
**Exactly one row per discrete clickstream product view event** (`event_id`).

#### 7.6.2 Schema Specification

| Column Name | Physical Data Type | Nullable | Key Type | Description / Business Invariants |
| :--- | :--- | :--- | :--- | :--- |
| `view_key` | `BIGINT` | `NOT NULL` | **PK** | Monotonically increasing surrogate key |
| `event_id` | `UUID` | `NOT NULL` | **NK** | Operational unique event UUID from Kafka event header |
| `product_key` | `BIGINT` | `NOT NULL` | **FK** | Foreign key linking to viewed item in `dim_products` |
| `customer_key` | `BIGINT` | `NOT NULL` | **FK** | Foreign key linking to `dim_customers` (`-1` for guests) |
| `date_key` | `INTEGER` | `NOT NULL` | **FK** | Foreign key linking to interaction date in `dim_date` |
| `session_token` | `VARCHAR(255)`| `NOT NULL` | Attribute | Guest/Authenticated browser session identifier |
| `view_timestamp`| `TIMESTAMPTZ` | `NOT NULL` | Temporal | Exact UTC timestamp of the view interaction |
| `view_count` | `INTEGER` | `NOT NULL` | Measure | Additive unit counter (always fixed at `1`) |

#### 7.6.3 Anonymous Guest Handling & Deduplication
- **Guest Support:** E-commerce traffic is heavily dominated by anonymous visitors. If `customer_id` is null in the `PRODUCT_VIEWED` event, `fact_product_views.customer_key` is assigned the reserved surrogate key `-1` (`Anonymous Guest`).
- **Session Attribution:** `session_token` captures browser session continuity, allowing funnel analysis (View $\rightarrow$ Add to Cart $\rightarrow$ Checkout) even before a user registers or logs in.
- **Clickstream Deduplication:** Deduplicating on `event_id` ensures network retries, browser double-clicks, or Kafka consumer restarts do not artificially inflate product view counts.

---

## 8. Surrogate Key vs. Natural Key Strategy

### 8.1 Dual-Key Design Rationale
The platform maintains a deliberate architectural separation between operational identifiers and analytical identifiers:
1. **OLTP Layer (PostgreSQL 16): Natural / Distributed UUIDs**
   - Natural business keys (`users.id`, `orders.id`, `products.id`, `payments.id`) utilize 128-bit UUIDs (v4/v7).
   - **Operational Advantages:** Globally unique across microservices, eliminator of centralized sequence coordination bottlenecks, safe for concurrent client-side pre-generation, and zero risk of ID collisions during distributed data migrations.
2. **Analytical Gold Layer: Surrogate Integer Keys**
   - Analytical models replace wide UUID strings with compact surrogate integers (`BIGINT` / `INTEGER`).
   - **Analytical Advantages:**
     - **Memory & Cache Footprint:** An 8-byte integer consumes 50% less space than a 16-byte binary UUID and 78% less space than a 36-byte formatted UUID string. In high-volume fact tables with millions of rows, joining on compact integer keys dramatically reduces CPU cache misses.
     - **Columnar Compression:** Columnar storage engines (Parquet, DuckDB, ClickHouse) achieve order-of-magnitude higher compression ratios on sequential integers via dictionary and delta encoding.
     - **Operational Decoupling:** Insulates the analytical warehouse from upstream operational schema migrations, database split-brain merges, or key restructuring.
     - **Special Sentinel Keys:** Enables standard Kimball sentinel values (e.g., `-1` for `Unknown / Anonymous Guest`, `-2` for `Not Applicable`), maintaining strict relational integrity without violating foreign key constraints.

### 8.2 Master Key Mapping Table

| Dimensional Entity | Natural Business Key | Operational Source Column | Surrogate Key | Analytical Data Type | Surrogate Allocation Semantic |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Date** | `calendar_date` | Date Continuum | `date_key` | `INTEGER` | Deterministic `YYYYMMDD` integer |
| **Customer** | `customer_id` | `users.id` (`UUID`) | `customer_key` | `BIGINT` | Stable integer identifier in `dim_customers` |
| **Product** | `product_id`, `sku` | `products.id` (`UUID`) | `product_key` | `BIGINT` | Stable integer identifier in `dim_products` |
| **Category** | `category_id`, `slug`| `categories.id` (`UUID`) | `category_key` | `BIGINT` | Stable integer identifier in `dim_categories` |
| **Order Status** | `status_code` | `orders.status` (`VARCHAR`)| `status_key` | `INTEGER` | Static Map (`1` to `5`) |
| **Order Fact** | `order_id` | `orders.id` (`UUID`) | `order_key` | `BIGINT` | Stable integer identifier in `fact_orders` |
| **Order Item Fact**| `order_item_id` | `order_items.id` (`UUID`)| `order_item_key` | `BIGINT` | Stable integer identifier in `fact_order_items` |
| **Payment Fact** | `payment_id` | `payments.id` (`UUID`) | `payment_key` | `BIGINT` | Stable integer identifier in `fact_payments` |
| **Product View Fact**| `event_id` | `event_id` (`UUID`) | `view_key` | `BIGINT` | Stable integer identifier in `fact_product_views` |

> [!NOTE]
> **Surrogate Key Generation Policy:** Surrogate keys are stable integer identifiers within Gold dimensions. The physical surrogate-key generation mechanism (e.g., persistent lookup tables, hash-based surrogates, or stateful streaming sequences) is implementation-specific and will be finalized in Phase 7.

---

## 9. Measure Categorization

### 9.1 Measure Classification Taxonomy
To safeguard analytical correctness, every quantitative metric in the data warehouse is classified into one of three behavioral categories:

```text
               +-------------------------------------------------+
               |              ANALYTICAL MEASURES                |
               +-------------------------------------------------+
                        │               │                │
            Fully Additive    Semi-Additive        Non-Additive
            (Safe for SUM     (Restricted SUM      (Never Sum Directly;
             across all dims)  across select dims)  Derived via Formulas)
```

### 9.2 Additive Measures
Additive measures can be meaningfully summed (`SUM()`) across **any and all dimensions** (Date, Product, Category, Customer, Order Status).

| Measure Name | Host Fact Table | Data Type | Permissible Aggregations | Business Meaning |
| :--- | :--- | :--- | :--- | :--- |
| `subtotal_amount` | `fact_orders` | `NUMERIC(12,2)` | `SUM`, `MIN`, `MAX` | Aggregate item merchandise value |
| `tax_amount` | `fact_orders` | `NUMERIC(12,2)` | `SUM`, `MIN`, `MAX` | Total collected sales tax |
| `shipping_amount` | `fact_orders` | `NUMERIC(12,2)` | `SUM`, `MIN`, `MAX` | Total collected shipping freight |
| `total_amount` | `fact_orders` | `NUMERIC(12,2)` | `SUM`, `MIN`, `MAX` | Total gross order commitment |
| `gmv_amount` | `fact_orders` | `NUMERIC(12,2)` | `SUM`, `MIN`, `MAX` | **Order-Level Recognized GMV:** Recognized solely upon `ORDER_PAID` |
| `quantity` | `fact_order_items` | `INTEGER` | `SUM`, `MIN`, `MAX` | Total physical product units sold |
| `line_total_amount`| `fact_order_items`| `NUMERIC(12,2)` | `SUM`, `MIN`, `MAX` | **Product Merchandise Sales:** Line merchandise sales (`unit_price * qty`) |
| `payment_amount` | `fact_payments` | `NUMERIC(12,2)` | `SUM`, `MIN`, `MAX` | Total dollar volume submitted to gateway |
| `view_count` | `fact_product_views`| `INTEGER` | `SUM`, `COUNT` | Total clickstream impression count |

> [!IMPORTANT]
> **Order-Level GMV vs. Product Merchandise Sales:**
> - **Order-Level GMV (`fact_orders.gmv_amount`):** Equals the total financial commitment of paid orders (`total_amount = subtotal + tax + shipping`).
> - **Product Merchandise Sales (`fact_order_items.line_total_amount`):** Represents merchandise revenue allocated to specific product line items and excludes order-level tax and shipping.

### 9.3 Semi-Additive Measures
Semi-additive measures can be summed across some dimensions (e.g., Products, Geography, Categories), but **cannot be summed across the temporal dimension (`dim_date`)**.
- **Platform Application:** In warehouse extensions tracking periodic daily inventory levels (`daily_inventory_snapshot`), stock levels can be summed across multiple product SKUs on a single date, but summing inventory across 30 days of the month is mathematically meaningless. For time dimensions, semi-additive measures require `END OF PERIOD` or `CURRENT` balance logic.

### 9.4 Non-Additive Measures & The `AVG()` Anti-Pattern
Non-additive measures **cannot be meaningfully summed across any dimension**. They must always be dynamically derived as ratios of additive components.

| Metric | Classification | Mathematical Definition | Fatal Anti-Pattern (DO NOT DO) |
| :--- | :--- | :--- | :--- |
| **Average Order Value (AOV)** | Non-Additive | $\frac{\sum \text{gmv\_amount}}{\text{COUNT(DISTINCT paid orders)}}$ | `AVG(total_amount)` across mixed order statuses |
| **Payment Gateway Success Rate**| Non-Additive | $\frac{\text{COUNT(SUCCESS attempts)}}{\text{COUNT(Total attempts)}}$ | Averaging daily success percentage values |
| **View-to-Unit-Purchase Ratio** | Non-Additive | $\frac{\sum \text{quantity}}{\sum \text{view\_count}}$ | Averaging individual product conversion ratios |
| **Order Cancellation Rate** | Non-Additive | $\frac{\text{COUNT(CANCELLED orders)}}{\text{COUNT(Total created orders)}}$ | Averaging monthly cancellation percentages |
| **Unit Price Snapshot** | Non-Additive | Price per discrete unit | `SUM(unit_price_snapshot)` across order items |

> [!CAUTION]
> **The `AVG(AVG(...))` Fallacy:** Never store pre-calculated averages in aggregate rollup tables. Averaging previously calculated averages produces mathematically invalid results whenever the underlying slice counts differ. Always store separate `SUM(numerator)` and `SUM(denominator)` additive columns, evaluating the division dynamically at query time.

---

## 10. Business Metrics & Semantic Calculation Framework

### 10.1 Sales & Commercial Revenue Metrics

#### 10.1.1 Gross Merchandise Value (GMV)
- **Business Meaning:** Total economic dollar value of merchandise and order commitments successfully sold and paid for through the platform.
- **Semantic Contract:**
  $$\text{GMV} = \sum_{\text{fact\_orders}} \text{gmv\_amount} \quad \text{WHERE } \text{is\_paid} = \text{TRUE}$$
- **Boundary Condition:** Excludes unpaid orders, pending checkouts, failed payments, and orders cancelled prior to payment confirmation. `ORDER_PAID` is the sole authoritative trigger.

#### 10.1.2 Gross Bookings vs. Net Product Merchandise Revenue
- **Gross Order Bookings:** Total committed checkout volume, regardless of eventual payment outcome:
  $$\text{Gross Bookings} = \sum_{\text{fact\_orders}} \text{total\_amount}$$
- **Net Product Merchandise Revenue:** Subtotal of physical merchandise sold (excluding freight shipping and taxes):
  $$\text{Net Product Merchandise Revenue} = \sum_{\text{fact\_orders}} \text{subtotal\_amount} \quad \text{WHERE } \text{is\_paid} = \text{TRUE}$$

#### 10.1.3 Average Order Value (AOV)
- **Business Meaning:** Mean dollar spend per successfully finalized commercial transaction.
- **Formula:**
  $$\text{AOV} = \frac{\sum_{\text{fact\_orders}} \text{gmv\_amount}}{\text{COUNT}(\text{DISTINCT } \text{order\_key}) \quad \text{WHERE } \text{is\_paid} = \text{TRUE}}$$

#### 10.1.4 Paid Order Conversion Rate
- **Business Meaning:** Proportion of placed orders that successfully clear financial settlement.
- **Formula:**
  $$\text{Paid Order Rate} = \frac{\text{COUNT}(\text{DISTINCT } \text{order\_key} \text{ WHERE } \text{is\_paid} = \text{TRUE})}{\text{COUNT}(\text{DISTINCT } \text{order\_key})}$$

---

### 10.2 Product & Merchandise Dynamics

#### 10.2.1 Top Selling Products by Merchandise Sales & Volume
- **Top by Merchandise Sales:**
  $$\text{Product Merchandise Sales}(p) = \sum_{i \in \text{fact\_order\_items}} i.\text{line\_total\_amount} \quad \text{WHERE } i.\text{product\_key} = p \land i.\text{order}.\text{is\_paid} = \text{TRUE}$$
  *(Note: Represents merchandise line sales and excludes order-level tax and shipping).*
- **Top by Volume:**
  $$\text{Product Units Sold}(p) = \sum_{i \in \text{fact\_order\_items}} i.\text{quantity} \quad \text{WHERE } i.\text{product\_key} = p \land i.\text{order}.\text{is\_paid} = \text{TRUE}$$

#### 10.2.2 Product View Popularity
- **Total Product Page Views:**
  $$\text{Product Views}(p) = \sum_{v \in \text{fact\_product\_views}} v.\text{view\_count} \quad \text{WHERE } v.\text{product\_key} = p$$

#### 10.2.3 View-to-Unit-Purchase Ratio & High-View Low-Conversion Detection
- **Aggregate Metric:**
  $$\text{View-to-Unit-Purchase Ratio}(p) = \frac{\text{Product Units Sold}(p)}{\text{Product Views}(p)}$$
- **Semantic Boundary:** The MVP metric is an aggregate view-to-unit-purchase ratio (units sold per product view), not a user-level attributed conversion funnel.
- **Anomaly Filter:** Flags products where $\text{Product Views}(p) \ge \text{Threshold}_{\text{traffic}}$ and $\text{View-to-Unit-Purchase Ratio}(p) \le \text{Threshold}_{\text{conversion}}$, isolating catalog items with strong browsing interest but poor purchasing conversion.

---

### 10.3 Category Dynamics
- **Category Merchandise Sales:**
  $$\text{Category Merchandise Sales}(c) = \sum \text{line\_total\_amount} \quad \text{WHERE } \text{category\_key} = c \land \text{is\_paid} = \text{TRUE}$$
- **Category Merchandise Mix Percentage:**
  $$\text{Category Share} = \frac{\text{Category Merchandise Sales}(c)}{\sum_{\text{all categories}} \text{Category Merchandise Sales}} \times 100\%$$

---

### 10.4 Customer Behavior & Retention
- **Active Transacting Customers:** Total unique registered customers who placed at least one paid order within the reporting period:
  $$\text{Active Buyers} = \text{COUNT}(\text{DISTINCT } \text{customer\_key}) \quad \text{WHERE } \text{is\_paid} = \text{TRUE} \land \text{customer\_key} \neq -1$$
- **Repeat Purchaser Rate:** Proportion of buyers who have completed two or more distinct paid orders in platform history:
  $$\text{Repeat Rate} = \frac{\text{COUNT}(\text{DISTINCT } c \text{ WHERE Paid Orders}(c) \ge 2)}{\text{COUNT}(\text{DISTINCT } c \text{ WHERE Paid Orders}(c) \ge 1)}$$
- **Customer Lifetime Value (LTV):** Cumulative GMV generated by an individual customer:
  $$\text{Customer LTV}(c) = \sum_{\text{fact\_orders}} \text{gmv\_amount} \quad \text{WHERE } \text{customer\_key} = c \land \text{is\_paid} = \text{TRUE}$$

---

### 10.5 Payment Gateway Performance & Health
- **Payment Attempt Success Rate:**
  $$\text{Gateway Success Rate} = \frac{\text{COUNT}(\text{payment\_key WHERE status = 'SUCCESS'})}{\text{COUNT}(\text{payment\_key})}$$
- **First-Attempt Payment Success Rate:**
  $$\text{First Attempt Pass Rate} = \frac{\text{COUNT}(\text{payment\_key WHERE status = 'SUCCESS' AND attempt\_number = 1})}{\text{COUNT}(\text{payment\_key WHERE attempt\_number = 1})}$$
- **Retry Success Recovery Rate:** Percentage of orders that initially failed payment but subsequently succeeded via customer retry:
  $$\text{Retry Recovery Rate} = \frac{\text{COUNT}(\text{DISTINCT } \text{order\_key WHERE attempt\_number > 1 AND status = 'SUCCESS'})}{\text{COUNT}(\text{DISTINCT } \text{order\_key WHERE attempt\_number > 1})}$$

---

### 10.6 End-to-End Conversion Funnel
The platform tracks shopper progression across 5 distinct lifecycle milestones:

```text
  [Step 1: Product View]          fact_product_views (view_count)
            │
            ▼ (View-to-Cart Drop-off)
  [Step 2: Cart Activity]         Silver silver_cart_events (item additions - staging/helper)
            │
            ▼ (Cart-to-Checkout Abandonment)
  [Step 3: Order Created]         fact_orders (status = PENDING)
            │
            ▼ (Payment Failure / Rejection)
  [Step 4: Order Paid (GMV)]      fact_orders (status = PAID, is_paid = TRUE via ORDER_PAID)
            │
            ▼ (Fulfillment & Delivery)
  [Step 5: Order Completed]       fact_orders (status = COMPLETED)
```

---

## 11. Event $\rightarrow$ Silver $\rightarrow$ Gold Mapping

### 11.1 Master Event Ingestion Matrix

| Upstream Domain Event | Kafka Topic Name | Silver Entity Target | Gold Dimensional Target | Transformation & Analytical Role |
| :--- | :--- | :--- | :--- | :--- |
| `USER_REGISTERED` | `ecommerce.users` | `silver_customers` | `dim_customers` | Ingests customer profiles; allocates surrogate `customer_key`. |
| `PRODUCT_VIEWED` | `ecommerce.views` | `silver_product_views` | `fact_product_views` | Ingests clickstream events; resolves guest vs. auth sessions; tracks impressions. |
| `ITEM_ADDED` | `ecommerce.carts` | `silver_cart_items` | *Staging / Helper Dataset*| Monitors cart intent; powers funnel drop-off analytics. Not a canonical Gold fact. |
| `CART_UPDATED` | `ecommerce.carts` | `silver_cart_events` | *Staging / Helper Dataset*| Monitors cart adjustments and abandonments. Not a canonical Gold fact. |
| `ORDER_CREATED` | `ecommerce.orders`| `silver_orders`, `silver_order_items` | `fact_orders`, `fact_order_items` | Inserts new order and lines; snapshots unit prices; sets status `PENDING`, GMV `0.00`. |
| `PAYMENT_FAILED` | `ecommerce.payments`| `silver_payments`, `silver_order_state` | `fact_payments`, `fact_orders` | Inserts failed attempt audit row in `fact_payments`; updates `fact_orders.status_key = PAYMENT_FAILED`. |
| `PAYMENT_COMPLETED` | `ecommerce.payments`| `silver_payments` | `fact_payments` | Inserts successful attempt audit row in `fact_payments`. **Does NOT recognize GMV.** |
| `ORDER_PAID` | `ecommerce.orders`| `silver_order_state` | `fact_orders` | **Sole GMV Recognition Event.** Sets `is_paid = TRUE`, `gmv_amount = total_amount`, status `PAID`. |
| `ORDER_CANCELLED` | `ecommerce.orders`| `silver_order_state` | `fact_orders` | Updates `fact_orders.status_key = CANCELLED`, sets `cancelled_at`. GMV remains `0.00`. |

> [!IMPORTANT]
> **Cart Funnel Datasets are Staging / Helper Datasets:**
> `silver_cart_items` and `silver_cart_events` are analytical helper/staging datasets used for funnel analysis. They are not part of the four canonical Gold fact tables (`fact_orders`, `fact_order_items`, `fact_payments`, `fact_product_views`) in the MVP Star Schema.

### 11.2 Handling Terminal Order Fulfillment (`COMPLETED`)
- **Fulfillment Contract & Boundary:**
  ```text
  COMPLETED is currently an OLTP order state defined by the order
  state machine, but the current MVP event contract does not define
  a dedicated ORDER_COMPLETED event or CDC source.

  Therefore Phase 6 does not assume a new ORDER_COMPLETED event.
  The physical ingestion mechanism for COMPLETED state changes must
  be defined in Phase 7 only if required by the existing architecture.
  ```
- **Architectural Guardrail:** The data model strictly prohibits adding unapproved Kafka topics, CDC mechanisms, or artificial events. When the physical synchronization of `COMPLETED` is implemented in Phase 7, it will update `fact_orders.status_key = 5` and record `completed_at`, inheriting the paid status and GMV previously recognized from `ORDER_PAID`.

---

## 12. Late-Arriving Events & Out-of-Order Handling

### 12.1 The Distributed Reality
In real-time distributed platforms, events frequently arrive out of chronological order due to mobile network reconnects, Kafka consumer rebalances, temporary worker node failures, or batch pipeline latency.

### 12.2 Event Time vs. Ingestion Time
- **Event Timestamp (`event_timestamp`):** The exact UTC moment the user interaction or business mutation occurred in the application domain. Stored in the event payload.
- **Ingestion Timestamp (`ingestion_timestamp`):** The UTC moment the message was written to the Bronze data lake.
- **Governing Rule:** 
  - `order_date_key` represents the calendar date the order was created.
  - `paid_date_key` represents the calendar date the `ORDER_PAID` event occurred.

### 12.3 Late-Arriving Event Policies & Partition Updates
1. **Late Event $\neq$ Duplicate Event:** A payment event arriving 6 hours after an order was created is a valid late-arriving event, not a duplicate. It must be processed and applied to the existing order record.
2. **Replay $\neq$ Double-Counting:** Re-running an ingestion pipeline across historical Bronze files must overwrite or update existing Gold facts deterministically, never appending duplicate financial entries.
3. **Multi-Partition Impact of Late `ORDER_PAID`:**
   ```text
   Late-arriving lifecycle events must update the existing order fact
   using order_id as the natural key.

   The pipeline must recompute or MERGE all affected analytical
   partitions/views, including the order-date partition and any
   paid-date-based aggregates affected by ORDER_PAID.

   The exact physical implementation is deferred to Phase 7.
   ```

---

## 13. Idempotency & Deduplication Architecture

### 13.1 Layered Idempotency Keys

| Pipeline Tier | Entity Scope | Primary Idempotency Key | Deduplication Mechanism | Failure Scenario Mitigated |
| :--- | :--- | :--- | :--- | :--- |
| **Kafka Broker** | Event Envelope | `event_id` (`UUID`) | Producer Idempotence (`enable.idempotence=true`) | Network retry duplicates on Kafka publish |
| **Silver Tier** | Cleaned Events | `event_id` (`UUID`) | `ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingestion_timestamp ASC)` | Duplicate consumer polling / reprocessing |
| **Gold Orders** | `fact_orders` | `order_id` (`UUID`) | `MERGE ON target.order_id = source.order_id` | Pipeline retries, duplicate lifecycle events |
| **Gold Items** | `fact_order_items` | `(order_id, product_id)` / `order_item_id` | Deterministic Natural Key Merge | Order line re-ingestion, batch DAG restarts |
| **Gold Payments** | `fact_payments` | `payment_id` (`UUID`) | `MERGE ON target.payment_id = source.payment_id` | Webhook re-deliveries, payment retry replays |
| **Gold Views** | `fact_product_views` | `event_id` (`UUID`) | Ingestion Partition Overwrite on `event_id` | Clickstream network retries, browser duplicates |

### 13.2 Silver Deduplication Mechanics
During Silver pipeline execution, deduplication is enforced deterministically:
```sql
-- Conceptual Deduplication Logic in Silver Layer
WITH RankedEvents AS (
    SELECT 
        event_id,
        event_type,
        payload,
        event_timestamp,
        ingestion_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY event_id 
            ORDER BY ingestion_timestamp ASC
        ) AS dedup_rank
    FROM bronze_events
    WHERE event_date = :processing_date
)
SELECT * 
FROM RankedEvents 
WHERE dedup_rank = 1;
```

---

## 14. Slowly Changing Dimension (SCD) Strategy

### 14.1 SCD Type 1 Architectural Decision
In accordance with `docs/05-database-design.md`, the platform standardizes on **SCD Type 1 (In-Place Overwrite)** across all Gold dimensions for the MVP:
- `dim_customers` $\rightarrow$ SCD Type 1
- `dim_products` $\rightarrow$ SCD Type 1
- `dim_categories` $\rightarrow$ SCD Type 1

### 14.2 Mechanism: In-Place Overwrite
When customer profile data (e.g., `full_name`, `email`, `status`) or product attributes (e.g., `title`, `description`, `category_id`, `current_price`) change in PostgreSQL, the updated values overwrite the existing row matching the natural business key (`customer_id` or `product_id`). Surrogate keys (`customer_key`, `product_key`) remain static and permanent.

### 14.3 Preserving Historical Accuracy Under SCD Type 1
```text
Historical monetary/order-line values are protected by
immutable fact snapshots.

Historical dimensional attribution such as category
is intentionally subject to SCD Type 1 limitations.
```
- `fact_order_items` permanently retains `product_sku_snapshot`, `product_title_snapshot`, and `unit_price_snapshot`.
- If a product title changes from *"Ergonomic Mouse"* to *"Pro Gaming Mouse v2"* and its price increases from \$50 to \$70 in `dim_products`, all past orders retain the \$50 price and *"Ergonomic Mouse"* title. Historical revenue calculations remain perfectly accurate.

### 14.4 Documented Trade-Offs & Limitations
- **Customer Relocation:** If customer geography or address attributes are added in future iterations, SCD Type 1 will attribute historical orders to the customer's newest address.
- **Category Reorganization Limitation:** If a product is reassigned from Category A to Category B, historical sales volume for that product will roll up into Category B under SCD Type 1 queries. Historical category attribution is intentionally subject to SCD Type 1 behavior; category-based historical analytics represent the current product-to-category relationship, not the historical assignment at order time.

---

## 15. Star Schema Architecture Diagram

### 15.1 Entity-Relationship Star Schema (Mermaid)

```mermaid
erDiagram
    DIM_DATE ||--o{ FACT_ORDERS : "order_date_key"
    DIM_DATE ||--o{ FACT_ORDERS : "paid_date_key"
    DIM_DATE ||--o{ FACT_ORDER_ITEMS : "order_date_key"
    DIM_DATE ||--o{ FACT_PAYMENTS : "payment_date_key"
    DIM_DATE ||--o{ FACT_PRODUCT_VIEWS : "date_key"

    DIM_CUSTOMERS ||--o{ FACT_ORDERS : "customer_key"
    DIM_CUSTOMERS ||--o{ FACT_PAYMENTS : "customer_key"
    DIM_CUSTOMERS |o--o{ FACT_PRODUCT_VIEWS : "customer_key"

    DIM_ORDER_STATUS ||--o{ FACT_ORDERS : "status_key"

    DIM_CATEGORIES ||--o{ DIM_PRODUCTS : "category_key"

    DIM_PRODUCTS ||--o{ FACT_ORDER_ITEMS : "product_key"
    DIM_PRODUCTS ||--o{ FACT_PRODUCT_VIEWS : "product_key"

    FACT_ORDERS ||--o{ FACT_ORDER_ITEMS : "order_key"
    FACT_ORDERS ||--o{ FACT_PAYMENTS : "order_key"

    DIM_DATE {
        int date_key PK
        date calendar_date NK
        smallint day_of_month
        smallint day_of_week
        string day_name
        boolean is_weekend
        smallint week_of_year
        smallint month_number
        string month_name
        smallint quarter_number
        smallint year_number
    }

    DIM_CUSTOMERS {
        bigint customer_key PK
        uuid customer_id NK
        string email
        string username
        string full_name
        string role
        string status
        int registration_date_key FK
    }

    DIM_CATEGORIES {
        bigint category_key PK
        uuid category_id NK
        string name
        string slug
        uuid parent_category_id
    }

    DIM_PRODUCTS {
        bigint product_key PK
        uuid product_id NK
        string sku
        string title
        bigint category_key FK
        numeric current_price
        boolean is_active
    }

    DIM_ORDER_STATUS {
        int status_key PK
        string status_code NK
        string display_name
        boolean is_terminal
        boolean is_paid_eligible
    }

    FACT_ORDERS {
        bigint order_key PK
        uuid order_id NK
        bigint customer_key FK
        int order_date_key FK
        int paid_date_key FK
        int status_key FK
        numeric subtotal_amount
        numeric tax_amount
        numeric shipping_amount
        numeric total_amount
        numeric gmv_amount
        boolean is_paid
    }

    FACT_ORDER_ITEMS {
        bigint order_item_key PK
        bigint order_key FK
        bigint product_key FK
        int order_date_key FK
        string product_sku_snapshot
        string product_title_snapshot
        numeric unit_price_snapshot
        int quantity
        numeric line_total_amount
    }

    FACT_PAYMENTS {
        bigint payment_key PK
        uuid payment_id NK
        bigint order_key FK
        bigint customer_key FK
        int payment_date_key FK
        int attempt_number
        string status
        numeric payment_amount
        string currency
        string payment_method
        string error_code
    }

    FACT_PRODUCT_VIEWS {
        bigint view_key PK
        uuid event_id NK
        bigint product_key FK
        bigint customer_key FK
        int date_key FK
        string session_token
        timestamp view_timestamp
        int view_count
    }
```

---

## 16. Gold Transformation Logic

### 16.1 Conceptual Pipeline Stages
The transition from cleaned Silver tables to dimensional Gold marts follows a 5-stage deterministic pipeline executed via PySpark:

```text
  [Silver Extracted Data]
            │
            ▼
  [Stage 1: Business Invariant Validation]  (Discard or quarantine corrupted math)
            │
            ▼
  [Stage 2: Natural Key Deduplication]     (Enforce single active record per entity)
            │
            ▼
  [Stage 3: Surrogate Key Resolution]      (Join conformed dimensions; assign -1 for unknowns)
            │
            ▼
  [Stage 4: Business Measure Calculation]  (Apply GMV boundary logic, snapshot item prices)
            │
            ▼
  [Stage 5: Idempotent Atomic Load]        (Partition overwrite or SQL MERGE into Gold)
```

### 16.2 Step-by-Step Model Assembly

#### 16.2.1 Dimension Loading (`dim_customers`, `dim_products`, `dim_categories`)
1. **Extraction:** Read changed records from Silver entity streams (`silver_customers`, `silver_products`, `silver_categories`).
2. **Surrogate Key Resolution:** Left-join existing Gold dimension table on natural business key (`customer_id` or `product_id`).
   - If match found: Retain existing surrogate key (SCD Type 1 update).
   - If no match found: Assign a new stable surrogate integer identifier within Gold dimensions. (The physical generation mechanism is implementation-specific and will be finalized in Phase 7).
3. **Attribute Refresh:** Overwrite mutable attributes (`full_name`, `status`, `title`, `current_price`, `updated_at`) in place.
4. **Load:** Upsert into Gold Parquet dimensional table.

#### 16.2.2 Fact Table Loading (`fact_orders`, `fact_order_items`)
1. **Extraction:** Read order state transitions (`ORDER_CREATED`, `ORDER_PAID`, `ORDER_CANCELLED`, and operational fulfillment state updates) for the execution batch.
2. **Dimension Lookups:**
   - Lookup `customer_key` from `dim_customers` via `customer_id` (fallback to `-1` if not found).
   - Lookup `order_date_key` from `dim_date` using `DATE(created_at)`.
   - Lookup `status_key` from `dim_order_status` matching current order status.
3. **Measure Calculation & Authoritative GMV Assignment:**
   - **On `ORDER_PAID` Event:**
     - Set `is_paid = TRUE`.
     - Set `gmv_amount = total_amount`.
     - Set `paid_date_key = DATE(event_timestamp)`.
     - Set `status_key = 2 (PAID)`.
   - **On `COMPLETED` Order State Transition:**
     - Set `status_key = 5 (COMPLETED)`.
     - Set `completed_at = event_timestamp`.
     - `is_paid` and `gmv_amount` retain their previously recognized values from `ORDER_PAID`.
     - An order reaching `COMPLETED` without a prior `ORDER_PAID` event must never independently synthesize GMV.
   - **On `ORDER_CREATED`, `PAYMENT_FAILED`, or `ORDER_CANCELLED`:**
     - `is_paid = FALSE`.
     - `gmv_amount = 0.00`.
     - `paid_date_key = NULL`.
4. **Order Item Snapshotting:**
   - Historical order item attributes (`product_sku_snapshot`, `product_title_snapshot`, `unit_price_snapshot`) must originate from the order transaction snapshot captured at order creation time (from the order payload/Silver order representation).
   - Current product dimension attributes must never overwrite historical order item price/title/SKU snapshots.
   - Validate line math: `line_total_amount = unit_price_snapshot * quantity`.
5. **Atomic Commit:** Merge into `fact_orders` on `order_id` (updating the single physical row in place) and insert into `fact_order_items` partitioned by `order_date_key`.

#### 16.2.3 Fact Payment Loading (`fact_payments`)
1. **Extraction:** Read `silver_payments` containing both successful and failed payment gateway attempts.
2. **Surrogate Resolution:** Lookup `order_key` from `fact_orders` and `customer_key` from `dim_customers`.
3. **Append Loading:** Insert each discrete attempt into `fact_payments`. Deduplicate on `payment_id` using idempotent write operations.
   *(Multi-attempt retries append distinct rows in `fact_payments` while GMV is recognized exactly once in `fact_orders` via `ORDER_PAID`).*

#### 16.2.4 Fact Clickstream Loading (`fact_product_views`)
1. **Extraction:** Read clickstream view events from `silver_product_views`.
2. **Surrogate Resolution:**
   - Lookup `product_key` from `dim_products` via `product_id`.
   - Lookup `customer_key` from `dim_customers` via `customer_id` (assign `-1: Anonymous Guest` if null).
   - Lookup `date_key` from `dim_date` via `DATE(view_timestamp)`.
3. **Partition Overwrite:** Write into `fact_product_views` partitioned by `date_key`, deduplicating on `event_id`.

---

## 17. Data Quality (DQ) Rules & Gateways

### 17.1 Quality Assertion Framework
Data quality gates operate as circuit breakers during pipeline execution. Violations are classified into **Fatal Errors** (halting the pipeline) and **Quarantine Records** (redirected to dead-letter tables without halting pipeline progress).

### 17.2 Dimension Quality Rules

| Dimension | Column | Quality Check Constraint | Action on Failure |
| :--- | :--- | :--- | :--- |
| `dim_date` | `date_key` | `NOT NULL`, Unique, Format `YYYYMMDD` (`20240101` - `20301231`) | Fatal Pipeline Halt |
| `dim_date` | `calendar_date` | `NOT NULL`, Unique, Continuous calendar days | Fatal Pipeline Halt |
| `dim_customers` | `customer_key` | `NOT NULL`, Unique Primary Key | Fatal Pipeline Halt |
| `dim_customers` | `customer_id` | `NOT NULL`, Unique Natural Key, Valid UUID format consistent with upstream | Quarantine Record |
| `dim_customers` | `email` | `NOT NULL`, Valid email format regex | Quarantine Record |
| `dim_products` | `product_key` | `NOT NULL`, Unique Primary Key | Fatal Pipeline Halt |
| `dim_products` | `product_id` | `NOT NULL`, Unique Natural Key, Valid UUID format consistent with upstream | Quarantine Record |
| `dim_products` | `current_price`| `NOT NULL`, `current_price >= 0.00` | Quarantine Record |
| `dim_categories`| `category_key` | `NOT NULL`, Unique Primary Key | Fatal Pipeline Halt |
| `dim_order_status`| `status_key` | Fixed range (`1` to `5`), `NOT NULL` | Fatal Pipeline Halt |

### 17.3 Fact Quality Rules

| Fact Table | Measure / Column | Quality Check Constraint | Action on Failure |
| :--- | :--- | :--- | :--- |
| `fact_orders` | `order_id` | `NOT NULL`, Unique Business Key | Quarantine Record |
| `fact_orders` | Financial Invariant | `total_amount = subtotal_amount + tax_amount + shipping_amount` | Quarantine Record |
| `fact_orders` | Non-negativity | `subtotal_amount >= 0`, `tax_amount >= 0`, `shipping_amount >= 0` | Quarantine Record |
| `fact_orders` | GMV Invariant | `IF is_paid = TRUE THEN gmv_amount = total_amount ELSE gmv_amount = 0`| Quarantine Record |
| `fact_orders` | Referential Integrity| `customer_key IN (SELECT customer_key FROM dim_customers)` | Set to `-1 (Unknown)` |
| `fact_order_items`| Line Math Invariant| `line_total_amount = unit_price_snapshot * quantity` | Quarantine Record |
| `fact_order_items`| Quantity Bounds | `quantity > 0 AND quantity <= 100` | Quarantine Record |
| `fact_order_items`| Referential Integrity| `order_key IN (SELECT order_key FROM fact_orders)` | Quarantine Record |
| `fact_payments` | `payment_id` | `NOT NULL`, Unique Payment Key | Quarantine Record |
| `fact_payments` | `status` | `status IN ('SUCCESS', 'FAILED')` | Quarantine Record |
| `fact_payments` | `payment_amount` | `payment_amount >= 0.00` | Quarantine Record |
| `fact_payments` | `attempt_number` | `attempt_number >= 1` | Quarantine Record |
| `fact_product_views`| `event_id` | `NOT NULL`, Unique Event Key | Quarantine Record |
| `fact_product_views`| `view_count` | `view_count = 1` | Quarantine Record |
| `fact_product_views`| Temporal Bound | `view_timestamp <= CURRENT_TIMESTAMP + INTERVAL '5 minutes'` | Quarantine Record |

### 17.4 Quarantine Architecture & Alert Thresholds
- **Quarantine Storage:** Records failing quality constraints are serialized with full payload, failure error code, and failure timestamp into `/quarantine/{entity}/year=YYYY/month=MM/day=DD/`.
- **Alert Threshold:** If quarantine records exceed **0.1%** of total batch volume, the pipeline triggers an automated P2 alert and fails the Airflow task run to prevent silent data corruption.

---

## 18. Field-Level Data Lineage

### 18.1 End-to-End Lineage Matrix

| Operational Source (PostgreSQL) | Outbox Event Payload Key | Kafka Topic / Ingestion Path | Bronze Parquet Column | Silver Cleaned Column | Gold Star Schema Target Column |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `users.id` | `user_id` | `ecommerce.users` | `raw_payload.user_id` | `silver_customers.customer_id` | `dim_customers.customer_id` |
| `users.email` | `email` | `ecommerce.users` | `raw_payload.email` | `silver_customers.email` | `dim_customers.email` |
| `users.full_name` | `full_name` | `ecommerce.users` | `raw_payload.full_name` | `silver_customers.full_name` | `dim_customers.full_name` |
| `products.id` | `product_id` | PostgreSQL operational source $\rightarrow$ Phase 7 extraction | `raw_payload.product_id`| `silver_products.product_id` | `dim_products.product_id` |
| `products.sku` | `sku` | PostgreSQL operational source $\rightarrow$ Phase 7 extraction | `raw_payload.sku` | `silver_products.sku` | `dim_products.sku` |
| `products.price` | `price` | PostgreSQL operational source $\rightarrow$ Phase 7 extraction | `raw_payload.price` | `silver_products.price` | `dim_products.current_price` |
| `categories.name` | `category_name` | PostgreSQL operational source $\rightarrow$ Phase 7 extraction | `raw_payload.name` | `silver_categories.name` | `dim_categories.name` |
| `orders.id` | `order_id` | `ecommerce.orders` | `raw_payload.order_id` | `silver_orders.order_id` | `fact_orders.order_id` |
| `orders.customer_id`| `customer_id` | `ecommerce.orders` | `raw_payload.customer_id`| `silver_orders.customer_id` | `fact_orders.customer_key` (via Dim Lookup) |
| `orders.total_amount`| `total_amount` | `ecommerce.orders` | `raw_payload.total_amount`| `silver_orders.total_amount` | `fact_orders.total_amount` |
| `orders.status` | `status` | `ecommerce.orders` | `raw_payload.status` | `silver_orders.status` | `fact_orders.status_key` (via Dim Lookup) |
| `orders.total_amount`| `total_amount` | `ORDER_PAID` | `raw_payload.total_amount`| `silver_order_state.amount` | `fact_orders.gmv_amount` (when `ORDER_PAID` arrives) |
| `order_items.id` | `order_item_id` | `ecommerce.orders` | `raw_payload.items[].id` | `silver_order_items.item_id` | `fact_order_items.order_item_key` |
| `order_items.product_sku`| `product_sku`| `ecommerce.orders` | `raw_payload.items[].sku` | `silver_order_items.sku` | `fact_order_items.product_sku_snapshot` |
| `order_items.unit_price`| `unit_price` | `ecommerce.orders` | `raw_payload.items[].price`| `silver_order_items.price`| `fact_order_items.unit_price_snapshot` |
| `order_items.quantity`| `quantity` | `ecommerce.orders` | `raw_payload.items[].qty` | `silver_order_items.quantity`| `fact_order_items.quantity` |
| `payments.id` | `payment_id` | `ecommerce.payments` | `raw_payload.payment_id` | `silver_payments.payment_id` | `fact_payments.payment_id` |
| `payments.attempt_number`|`attempt` | `ecommerce.payments` | `raw_payload.attempt` | `silver_payments.attempt` | `fact_payments.attempt_number` |
| `payments.status` | `status` | `ecommerce.payments` | `raw_payload.status` | `silver_payments.status` | `fact_payments.status` |
| `payments.amount` | `amount` | `ecommerce.payments` | `raw_payload.amount` | `silver_payments.amount` | `fact_payments.payment_amount` |
| *Clickstream Client*| `event_id` | `ecommerce.views` | `raw_payload.event_id` | `silver_product_views.event_id`| `fact_product_views.event_id` |
| *Clickstream Client*| `session_token` | `ecommerce.views` | `raw_payload.session` | `silver_product_views.session` | `fact_product_views.session_token` |

---

## 19. Analytical Query Reference Implementations (Pseudo-SQL)

The following reference SQL queries demonstrate how the analytical data model directly answers primary business questions without requiring joins against operational OLTP tables.

### 19.1 Query 1: Daily GMV, Total Orders, and Paid Conversion Rate

```sql
-- Daily Commercial Revenue and Settlement Health
SELECT 
    d.calendar_date,
    COUNT(DISTINCT fo.order_key) AS total_orders_placed,
    COUNT(DISTINCT CASE WHEN fo.is_paid = TRUE THEN fo.order_key END) AS total_paid_orders,
    ROUND(
        COUNT(DISTINCT CASE WHEN fo.is_paid = TRUE THEN fo.order_key END)::NUMERIC / 
        NULLIF(COUNT(DISTINCT fo.order_key), 0) * 100.0, 2
    ) AS paid_order_conversion_pct,
    SUM(fo.total_amount) AS gross_order_bookings,
    SUM(fo.gmv_amount) AS recognized_gmv,
    ROUND(
        SUM(fo.gmv_amount) / 
        NULLIF(COUNT(DISTINCT CASE WHEN fo.is_paid = TRUE THEN fo.order_key END), 0), 2
    ) AS average_order_value_aov
FROM fact_orders fo
JOIN dim_date d ON fo.order_date_key = d.date_key
WHERE d.calendar_date BETWEEN '2026-09-01' AND '2026-09-25'
GROUP BY d.calendar_date
ORDER BY d.calendar_date DESC;
```

---

### 19.2 Query 2: Top 10 Best-Selling Products by Merchandise Sales and Units Sold

```sql
-- Top 10 Products by Merchandise Sales (Excludes Order-Level Tax & Shipping)
SELECT 
    dp.product_key,
    dp.sku AS catalog_sku,
    foi.product_title_snapshot AS product_title,
    dc.name AS category_name,
    SUM(foi.quantity) AS total_units_sold,
    SUM(foi.line_total_amount) AS product_merchandise_sales
FROM fact_order_items foi
JOIN fact_orders fo ON foi.order_key = fo.order_key
JOIN dim_products dp ON foi.product_key = dp.product_key
JOIN dim_categories dc ON dp.category_key = dc.category_key
WHERE fo.is_paid = TRUE
GROUP BY dp.product_key, dp.sku, foi.product_title_snapshot, dc.name
ORDER BY product_merchandise_sales DESC
LIMIT 10;
```

---

### 19.3 Query 3: High-View Low-Conversion Products (Funnel Drop-Off)

```sql
-- Anomaly Detection: Products with High Shopper Views but Poor Sales Conversion
WITH ProductViews AS (
    SELECT 
        product_key,
        SUM(view_count) AS total_page_views
    FROM fact_product_views
    GROUP BY product_key
),
ProductSales AS (
    SELECT 
        foi.product_key,
        SUM(foi.quantity) AS total_units_sold,
        SUM(foi.line_total_amount) AS product_merchandise_sales
    FROM fact_order_items foi
    JOIN fact_orders fo ON foi.order_key = fo.order_key
    WHERE fo.is_paid = TRUE
    GROUP BY foi.product_key
)
SELECT 
    dp.product_key,
    dp.sku,
    dp.title,
    dp.current_price,
    pv.total_page_views,
    COALESCE(ps.total_units_sold, 0) AS total_units_sold,
    COALESCE(ps.product_merchandise_sales, 0.00) AS product_merchandise_sales,
    ROUND(
        COALESCE(ps.total_units_sold, 0)::NUMERIC / 
        NULLIF(pv.total_page_views, 0) * 100.0, 3
    ) AS view_to_unit_purchase_ratio_pct
FROM ProductViews pv
JOIN dim_products dp ON pv.product_key = dp.product_key
LEFT JOIN ProductSales ps ON pv.product_key = ps.product_key
WHERE pv.total_page_views >= 500  -- Statistical significance threshold
ORDER BY view_to_unit_purchase_ratio_pct ASC, pv.total_page_views DESC
LIMIT 20;
```

---

### 19.4 Query 4: Payment Gateway Failure Rate and Retry Success Recovery

```sql
-- Gateway Reliability, Error Code Distribution, and Retry Recovery Rate
SELECT 
    d.calendar_date,
    COUNT(fp.payment_key) AS total_gateway_attempts,
    COUNT(CASE WHEN fp.status = 'SUCCESS' THEN 1 END) AS successful_attempts,
    COUNT(CASE WHEN fp.status = 'FAILED' THEN 1 END) AS failed_attempts,
    ROUND(
        COUNT(CASE WHEN fp.status = 'FAILED' THEN 1 END)::NUMERIC / 
        NULLIF(COUNT(fp.payment_key), 0) * 100.0, 2
    ) AS failure_rate_pct,
    COUNT(DISTINCT CASE WHEN fp.attempt_number > 1 AND fp.status = 'SUCCESS' THEN fp.order_key END) AS retry_recovered_orders,
    ROUND(
        COUNT(DISTINCT CASE WHEN fp.attempt_number > 1 AND fp.status = 'SUCCESS' THEN fp.order_key END)::NUMERIC /
        NULLIF(COUNT(DISTINCT CASE WHEN fp.attempt_number > 1 THEN fp.order_key END), 0) * 100.0, 2
    ) AS retry_recovery_success_pct
FROM fact_payments fp
JOIN dim_date d ON fp.payment_date_key = d.date_key
GROUP BY d.calendar_date
ORDER BY d.calendar_date DESC;
```

---

### 19.5 Query 5: Customer Retention & Repeat Purchaser Cohort Analysis

```sql
-- Customer Spending Concentration & Repeat Buyer Frequency (Paid Orders Only)
WITH CustomerOrderSummary AS (
    SELECT 
        customer_key,
        COUNT(order_key) AS paid_order_count,
        SUM(gmv_amount) AS lifetime_gmv,
        MIN(created_at) AS first_order_timestamp,
        MAX(created_at) AS latest_order_timestamp
    FROM fact_orders
    WHERE is_paid = TRUE AND customer_key <> -1
    GROUP BY customer_key
)
SELECT 
    CASE 
        WHEN paid_order_count = 1 THEN '1. One-Time Buyer'
        WHEN paid_order_count BETWEEN 2 AND 4 THEN '2. Repeat Buyer (2-4)'
        ELSE '3. High-Frequency VIP (5+)'
    END AS customer_cohort,
    COUNT(customer_key) AS total_customers,
    SUM(lifetime_gmv) AS cohort_cumulative_gmv,
    ROUND(AVG(lifetime_gmv), 2) AS average_customer_ltv
FROM CustomerOrderSummary
GROUP BY 1
ORDER BY 1 ASC;
```

---

### 19.6 Query 6: Operational Order Cancellation Rate Over Time

```sql
-- Tracking Operational Order Cancellation Rate Across Created Orders
SELECT 
    d.calendar_date,
    COUNT(fo.order_key) AS total_orders_created,
    COUNT(CASE WHEN dos.status_code = 'CANCELLED' THEN 1 END) AS total_orders_cancelled,
    ROUND(
        COUNT(CASE WHEN dos.status_code = 'CANCELLED' THEN 1 END)::NUMERIC / 
        NULLIF(COUNT(fo.order_key), 0) * 100.0, 2
    ) AS cancellation_rate_pct
FROM fact_orders fo
JOIN dim_date d ON fo.order_date_key = d.date_key
JOIN dim_order_status dos ON fo.status_key = dos.status_key
GROUP BY d.calendar_date
ORDER BY d.calendar_date DESC;
```
*(Note: Denominator is all orders created; measures operational cancellation rate of the placed order population, distinct from gateway payment failure rate).*

---

## 20. Semantic Definitions Reference Table

| Metric Name | Formal Business Definition | Mathematical Formula | Primary Source Fact Table | Mandatory Query Filter / Condition |
| :--- | :--- | :--- | :--- | :--- |
| **Gross Merchandise Value (GMV)** | Total monetary value of orders for which payment was confirmed via `ORDER_PAID`. | $\sum \text{gmv\_amount}$ | `fact_orders` | `is_paid = TRUE` |
| **Average Order Value (AOV)** | Average dollar spend per paid commercial order. | $\frac{\sum \text{gmv\_amount}}{\text{COUNT(DISTINCT order\_key)}}$ | `fact_orders` | `is_paid = TRUE` |
| **Paid Order Rate** | Percentage of placed orders that successfully convert to paid status. | $\frac{\text{Paid Orders}}{\text{Total Placed Orders}} \times 100$ | `fact_orders` | None |
| **Product Merchandise Sales** | Merchandise sales revenue generated by a product line item (excludes order tax & shipping). | $\sum \text{line\_total\_amount}$ | `fact_order_items` | `fact_orders.is_paid = TRUE` |
| **Product Units Sold** | Total physical quantity of a specific SKU purchased. | $\sum \text{quantity}$ | `fact_order_items` | `fact_orders.is_paid = TRUE` |
| **Product View Count** | Total clickstream impression count for a product page. | $\sum \text{view\_count}$ | `fact_product_views` | None |
| **View-to-Unit-Purchase Ratio** | Aggregate units sold per product view impression (not a user-level attributed funnel). | $\frac{\text{Units Sold}}{\text{Product Views}} \times 100$ | `fact_order_items`, `fact_product_views` | None |
| **Payment Gateway Success Rate** | Proportion of payment authorization attempts that succeeded. | $\frac{\text{SUCCESS Attempts}}{\text{Total Attempts}} \times 100$ | `fact_payments` | None |
| **First-Attempt Gateway Rate** | Proportion of initial payment attempts that succeeded on attempt 1. | $\frac{\text{SUCCESS Attempts (Attempt 1)}}{\text{Total Attempt 1 Records}} \times 100$ | `fact_payments` | `attempt_number = 1` |
| **Retry Recovery Rate** | Proportion of orders with multi-attempt failures that recovered. | $\frac{\text{Recovered Orders}}{\text{Orders with Multi-Attempts}} \times 100$ | `fact_payments` | `attempt_number > 1` |
| **Repeat Buyer Rate** | Proportion of active buyers who have completed $\ge 2$ paid orders. | $\frac{\text{Buyers with } \ge 2 \text{ Orders}}{\text{Total Unique Buyers}} \times 100$ | `fact_orders` | `is_paid = TRUE` |
| **Order Cancellation Rate** | Proportion of placed orders terminated prior to fulfillment. | $\frac{\text{CANCELLED Orders}}{\text{Total Placed Orders}} \times 100$ | `fact_orders` | None |

---

## 21. Known Limitations & Engineering Trade-Offs

1. **SCD Type 1 Attribute History Loss:**
   - *Limitation:* Overwriting customer names, active product catalog prices, and product category associations means historical reporting reflects the *current* dimension state.
   - *Mitigation:* Critical financial history is permanently preserved via fact snapshots (`unit_price_snapshot`, `product_title_snapshot`, `product_sku_snapshot` in `fact_order_items`). Category attribution is intentionally subject to SCD Type 1 behavior.
2. **Anonymous Guest Session Attribution Boundaries:**
   - *Limitation:* Clickstream views generated by guest users who never log in or register are attributed to the sentinel customer key `-1`. 
   - *Mitigation:* While cross-device persistent customer tracking is limited, intra-session conversion funnels remain fully queryable via `session_token`.
3. **Single Currency Model (USD):**
   - *Limitation:* All monetary measures (`total_amount`, `gmv_amount`, `payment_amount`) are hardcoded to `'USD'`. Multi-currency conversion tables and foreign exchange (FX) rates are omitted in the MVP.
4. **Synthetic Payment Mocking:**
   - *Limitation:* The payment gateway executes via an internal synchronous mock service. Real-world asynchronous webhook callbacks, chargebacks, and partial refunds are deferred to post-MVP roadmap iterations.
5. **Analytical Storage Engine Abstraction:**
   - *Limitation:* While the logical star schema is finalized, the physical storage engine (DuckDB embedded vs. ClickHouse distributed) is abstracted at the Parquet Lakehouse layer.

---

## 22. Open Questions & Architectural Design Notes

### 22.1 Status of Open Architectural Items
- **Resolved:** GMV boundary is locked strictly to `ORDER_PAID`. `PAYMENT_COMPLETED` is an audit event only.
- **Resolved:** Grains of all 4 facts and 5 dimensions are mathematically defined and locked.
- **Resolved:** Deduplication and idempotency keys are finalized across all layers.
- **Open Item (Non-Blocking):** Physical Analytical Query Engine Selection for Phase 7 implementation:
  - *Option A (DuckDB):* Ultra-fast, zero-overhead embedded vectorized SQL engine reading directly from Gold Parquet files on local NVMe storage. Highly recommended for 16 GB RAM local execution.
  - *Option B (ClickHouse):* High-concurrency, real-time distributed OLAP columnar database. Superior for multi-user sub-second BI dashboards, but requires running an additional daemon container.
  - *Resolution Path:* The logical Star Schema and Parquet partition layout designed herein are 100% compatible with both DuckDB and ClickHouse. The selection will be formalized in `docs/07-data-pipeline.md`.

---

## 23. Requirement Traceability Matrix

| Requirement ID | Addressed Domain / Business Rule | Use Case Alignment | Analytical Data Model Construct | Target Business Metric |
| :--- | :--- | :--- | :--- | :--- |
| `AR-001` | Real-time / Daily GMV Tracking | `UC-DATA-003`, `UC-ADMIN-004` | `fact_orders.gmv_amount`, `is_paid` | Gross Merchandise Value (GMV) |
| `AR-002` | Average Order Value (AOV) | `UC-DATA-003`, `UC-ADMIN-004` | `fact_orders.gmv_amount`, `order_key` | Average Order Value (AOV) |
| `AR-003` | Order Volume & Status Distribution | `UC-DATA-003`, `UC-ADMIN-004` | `fact_orders.status_key`, `dim_order_status` | Daily Order Volume, Cancellation Rate |
| `AR-004` | Best-Selling Products by Merchandise Sales & Units | `UC-DATA-003`, `UC-ADMIN-004` | `fact_order_items.line_total_amount`, `quantity` | Top Products by Merchandise Sales & Units |
| `AR-005` | Category Merchandise Sales & Mix | `UC-DATA-003`, `UC-ADMIN-004` | `fact_order_items.line_total_amount`, `dim_categories` | Category Merchandise Sales |
| `AR-006` | Conversion Funnel Drop-Off | `UC-DATA-003`, `UC-ADMIN-004` | `fact_product_views`, `fact_order_items` | View-to-Unit-Purchase Ratio |
| `AR-007` | High-View Low-Conversion Detection | `UC-DATA-003`, `UC-ADMIN-004` | `fact_product_views`, `fact_order_items` | Product Conversion Anomaly Score |
| `AR-008` | Active Transacting Customers | `UC-DATA-003`, `UC-ADMIN-004` | `fact_orders.customer_key`, `dim_customers` | Active Buyers Count |
| `AR-009` | Repeat Purchaser Rate | `UC-DATA-003`, `UC-ADMIN-004` | `fact_orders.customer_key`, `order_key` | Repeat Buyer Retention Rate |
| `AR-010` | Customer Lifetime Value (LTV) | `UC-DATA-003`, `UC-ADMIN-004` | `fact_orders.customer_key`, `gmv_amount` | Customer LTV Cohorts |
| `AR-011` | Payment Gateway Success & Error Analysis| `UC-DATA-003`, `UC-ADMIN-004` | `fact_payments.status`, `error_code` | Gateway Success Rate, Failure Code Breakdown |
| `AR-012` | Payment Retry Recovery Rate | `UC-DATA-003`, `UC-ADMIN-004` | `fact_payments.attempt_number`, `status` | Retry Recovery Conversion Rate |
| `BR-012` | Authoritative GMV Boundary Rule | `UC-009`, `UC-010`, `UC-DATA-003` | `fact_orders.is_paid = TRUE` via `ORDER_PAID`| Isolated Commercial Revenue Recognition |
| `DR-006` | Idempotent Analytical Pipelines | `UC-DATA-002`, `UC-DATA-005` | Deduplication on `event_id`, Merge on `order_id`| Replayable Zero-Duplicate Gold Marts |
| `DQ-001` - `DQ-005`| Silver & Gold Data Quality Rules | `UC-DATA-002`, `UC-DATA-005` | Automated Gateways & Quarantine Dead-Letter | Clean Dimensional Lakehouse |

---

## 24. Final Review Status

READY FOR DATA PIPELINE DESIGN

Consistency checks completed against:
- docs/01-project-brief.md
- docs/02-requirements.md
- docs/03-use-cases.md
- docs/04-system-architecture.md
- docs/05-database-design.md

Key invariants verified:
- ORDER_PAID is the sole GMV recognition boundary
- PAYMENT_COMPLETED is payment-attempt success only
- No unsupported ORDER_COMPLETED event was introduced
- Fact grains are stable
- Payment retries remain multiple payment facts but one GMV recognition
- SCD Type 1 limitations are explicitly documented
- Historical order-line snapshots remain immutable
- Product/category revenue terminology matches the actual measures
- Cart funnel datasets remain staging/helper datasets
- No analytical query depends on OLTP PostgreSQL
- Physical implementation details are deferred to Phase 7

### Next Deliverable
The logical analytical data model is frozen and verified. The immediate next deliverable is:

```text
docs/07-data-pipeline.md
```

`07-data-pipeline.md` will design the physical execution pipelines:
1. **Bronze Ingestion:** Kafka $\rightarrow$ Parquet batch/streaming consumer jobs.
2. **Silver Transformations:** PySpark JSON unpacking, schema enforcement, deduplication, and quarantine routing.
3. **Gold Mart Transformations:** PySpark dimensional modeling, surrogate key lookups, GMV calculation, and atomic partition upserts.
4. **Airflow Orchestration:** DAG structure, task dependencies, schedules, SLA monitoring, sensor waiting, backfills, and retry handling.
