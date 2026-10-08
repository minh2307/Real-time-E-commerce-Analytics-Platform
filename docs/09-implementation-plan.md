# 09 — Implementation Plan

## 1. Purpose
This document provides the definitive implementation plan for the **Real-time E-commerce Analytics Platform**. It bridges the gap between architectural design (Phase 1-8) and physical code execution, providing a phased roadmap, resolving technology choices, establishing strict module boundaries, and enforcing resource constraints for a 16 GB RAM development machine using independent runtime profiles.

## 2. Current Repository State
The repository has established its foundation, completed the data engineering pipeline (Bronze, Silver, Gold, DuckDB, Airflow), and implemented the Transactional Outbox.
- **Existing:** Spring Boot single-module backend foundation with ArchUnit and Flyway (V1-V8). Outbox domain implementation (`com.realtimeanalytics.outbox.*`). Fully functional PySpark and Airflow data pipeline using a mock event generator (`generate_e2e_data.py`).
- **Missing:** The core Java business domains (User, Catalog, Cart, Order, Payment) are not yet implemented. Real Application E2E integration is pending.

## 3. Architecture Decisions
The following decisions resolve TBDs from previous specifications to optimize for a local 16 GB RAM development environment.

### Backend Modularity Principle
- **Recommended Project Choice:** `ARCHITECTURAL DECISION` **Single Spring Boot application using package-by-feature / modular-monolith organization, with ArchUnit enforcing dependency boundaries.**
- **Reason:** The project uses a modular-monolith style within a single Spring Boot Gradle module. Business capabilities are separated by feature packages, and ArchUnit tests enforce architectural boundaries. Multi-module Gradle decomposition is intentionally deferred unless project complexity later justifies it. This prevents the repository from drifting back toward a large package-less monolith while keeping the build process simple.

### Analytical Store
- **Recommended Project Choice:** `ARCHITECTURAL DECISION` **DuckDB**
- **Reason:** DuckDB runs embedded within the Python process, requiring zero standing container memory overhead. ClickHouse requires a minimum of 2-4 GB RAM, which threatens the 16 GB system limit.
- **Impact:** The Gold layer will be queried using DuckDB directly over the Gold Parquet files.

### Kafka Serialization Format
- **Recommended Project Choice:** `ARCHITECTURAL DECISION` **JSON**
- **Reason:** JSON avoids the need for a standing Confluent Schema Registry container (saving ~1 GB RAM) and is easier to debug during local development.

### Schema Registry Requirement
- **Recommended Project Choice:** `ARCHITECTURAL DECISION` **None**
- **Reason:** See above. Schema evolution will be handled natively by PySpark schema inferences and strict Silver layer parsing.

### Java Version
- **Recommended Project Choice:** `ARCHITECTURAL DECISION` **Java 21**
- **Reason:** Long-Term Support (LTS) release with Virtual Threads, which highly optimizes the Outbox Poller and concurrent HTTP request handling without excessive memory consumption.

### Build Tool
- **Recommended Project Choice:** `ARCHITECTURAL DECISION` **Gradle (Kotlin DSL)**
- **Reason:** Faster incremental builds for a multi-module monolith, cleaner dependency management syntax.

### Database Migrations
- **Recommended Project Choice:** `ARCHITECTURAL DECISION` **Flyway**
- **Reason:** Simpler SQL-based migration scripts that map directly to the DDL specified in `05-database-design.md`.

### Docker Architecture
- **Recommended Project Choice:** `ARCHITECTURAL DECISION` **Single docker-compose.yml with explicit profiles**
- **Reason:** Running the full stack consumes ~15.5 GB RAM, leaving no room for the OS and IDE. Using explicit profiles (`backend`, `events`, `data-eng`) allows developers to selectively launch only the required infrastructure.

### Spark Deployment Mode
- **Recommended Project Choice:** `ARCHITECTURAL DECISION` **PySpark in Local Mode (`local[*]`)**
- **Reason:** Standing up a Spark Standalone cluster (Master + Workers) in Docker consumes too much RAM. Airflow will execute PySpark jobs locally within its container.

### Airflow Deployment Mode
- **Recommended Project Choice:** `ARCHITECTURAL DECISION` **Airflow LocalExecutor/SequentialExecutor (Single Container/Venv)**
- **Reason:** CeleryExecutor requires Redis and multiple worker containers. Using SequentialExecutor in a local python virtual environment directly avoids docker overhead and dramatically reduces RAM consumption for the local MVP.

### Data Quality Approach
- **Recommended Project Choice:** `ARCHITECTURAL DECISION` **PySpark Native Asserts**
- **Reason:** Great Expectations adds heavy dependencies and execution overhead. Native PySpark filtering and routing to `/quarantine/` is faster and uses less memory.

### Testing Strategy
- **Recommended Project Choice:** `ARCHITECTURAL DECISION` **JUnit 5 + Testcontainers + ArchUnit + PyTest**
- **Reason:** Standard, robust stack. Testcontainers will be used for Repository and E2E tests.

## 4. Remaining TBDs
- **JWT Library & Token Expiration:** To be decided during Spring Security implementation.
- **Frontend/UI:** Out of scope for this architecture, but API CORS configuration remains TBD.

## 5. Target Repository Structure
```text
.
├── backend/                  # Single Spring Boot Module (Java 21, Gradle)
│   ├── src/main/java/com/realtimeanalytics/
│   │   ├── user/             # api/, application/, domain/, infrastructure/
│   │   ├── catalog/          # api/, application/, domain/, infrastructure/
│   │   ├── cart/             # api/, application/, domain/, infrastructure/
│   │   ├── order/            # api/, application/, domain/, infrastructure/
│   │   ├── payment/          # api/, application/, domain/, infrastructure/
│   │   └── outbox/           # application/, domain/, infrastructure/
├── data-pipeline/            # PySpark Scripts & Airflow DAGs
│   ├── dags/
│   ├── spark_jobs/
│   ├── scripts/
│   │   └── generate_e2e_data.py # Mock event generator for pipeline testing
│   └── tests/
├── infra/                    # Docker Compose & configurations
│   ├── docker-compose.yml
│   ├── postgres/
│   └── kafka/
└── docs/                     # Source of Truth documentation
```

## 6. Modular Monolith Boundaries

Cross-module communication MUST happen through:
- Application services
- Public interfaces
- Ports

A module MUST NOT directly import another module's:
- Repository
- JPA repository
- Entity implementation
- Infrastructure adapter
- Infrastructure package

Example:
`order -> InventoryPort` (ALLOWED)
`order -> InventoryRepository` (FORBIDDEN)

## 7. Architecture Rules
`ARCHITECTURAL RULE` to be enforced via ArchUnit:
1. **Module Independence:** Classes in `backend.order.*` must not import classes from `backend.inventory.infrastructure.*` or `backend.inventory.repository.*`.
2. **Controller Isolation:** `@RestController` classes must not inject `@Repository` interfaces directly.
3. **Domain Purity:** Domain entities (classes inside `domain` packages) must not import Spring `@Autowired` or JPA `@Entity` (if using clean architecture) OR if using pragmatic JPA entities, they must not inject services.
4. **Outbox Enforcement:** Domain events must only be published via the Outbox interface, never directly to a Kafka producer.
5. **No Infrastructure Leakage:** Infrastructure concerns must remain isolated and not leak into the domain layer.

## 8. Infrastructure Strategy & Resource Budget

The 16 GB RAM limit cannot support all infrastructure and the IDE running simultaneously without aggressive swapping. The infrastructure is divided into explicit Docker Compose profiles. Do not assume all services must run simultaneously.

### Minimal Backend Profile (Profile: `backend`)
Used for local backend API development. Spring Boot runs directly from the host IDE.
- **PostgreSQL 16:** Max 2 GB RAM
- **Redis 7:** Max 0.5 GB RAM
*Total Container RAM: ~2.5 GB. Leaves ~13.5 GB for OS, IDE, and Spring Boot.*

### Event Profile (Profile: `events`)
Used for testing the Outbox poller and message broker integration.
- **Kafka (Kraft Mode):** Max 1 GB RAM
- (Requires `backend` profile)
*Total Container RAM: ~3.5 GB. Leaves ~12.5 GB for OS, IDE, and Spring Boot.*

### Data Engineering Profile (Profile: `data-eng`)
Used strictly for data pipeline development. Spring Boot should ideally be shut down to conserve memory.
- **Kafka Ingestion Daemon:** Max 0.5 GB RAM
- **Airflow LocalExecutor (running PySpark):** Max 4 GB RAM
- (Requires `events` and `backend` profiles)
*Total Container RAM: ~8 GB. Leaves ~8 GB for OS and basic IDE (VSCode/PyCharm).*

## 9. Database Implementation Strategy
**10 database tables implemented through 8 Flyway migrations.**

The 10 core tables specified in `05-database-design.md` are: `users`, `categories`, `products`, `inventory`, `carts`, `cart_items`, `orders`, `order_items`, `payments`, `outbox_events`.

Migration files were created in the following dependency order (V1 to V8):
1. `V1__create_users.sql` (Creates: `users`)
2. `V2__create_categories.sql` (Creates: `categories`)
3. `V3__create_products.sql` (Creates: `products`)
4. `V4__create_inventory.sql` (Creates: `inventory`)
5. `V5__create_carts.sql` (Creates: `carts`, `cart_items`)
6. `V6__create_orders.sql` (Creates: `orders`, `order_items`)
7. `V7__create_payments.sql` (Creates: `payments`)
8. `V8__create_outbox.sql` (Creates: `outbox_events`)

## 10. Event-Driven Strategy
- **Lifecycle:** Local DB Transaction $\rightarrow$ `outbox_events` $\rightarrow$ Poller $\rightarrow$ Kafka $\rightarrow$ Bronze.
- **Envelopes:** JSON payload with `event_id`, `event_type`, `aggregate_type`, `aggregate_id`, `occurred_at`.
- **Idempotency:** Upstream APIs require `Idempotency-Key` headers. Silver pipeline deduplicates by `event_id`.
- **GMV Semantics:** `ORDER_PAID` is the absolute trigger for GMV.

## 11. Data Pipeline Strategy
- **Bronze:** Append-only Parquet. Partitioned by `ingestion_date` and `hour`. No deduplication.
- **Silver:** PySpark batch jobs. Deduplication using `ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingestion_timestamp ASC)`. Quarantine of malformed JSON.
- **Gold:** Kimball Star Schema. In-place merges (`MERGE INTO`) on `order_id` to update `fact_orders` without appending duplicate rows on lifecycle changes.

## 12. Airflow Strategy
**DAG Structure:**
- `ecommerce_local_e2e`: Orchestrates the complete end-to-end data pipeline from mocking/ingesting events to Bronze, Silver, Gold transformations, and validating analytics via DuckDB.
- `generate_e2e_data.py`: A legitimate synthetic test-data utility. Its role is to generate deterministic/synthetic events for independent Data Engineering pipeline testing without requiring the Spring Boot application. This tool should remain available even after the real backend is implemented.

## 13. Testing Strategy
- **Unit Tests:** Domain logic, state machine transitions, cart math (JUnit 5).
- **Architecture Tests:** ArchUnit verifying module boundaries.
- **Repository Tests:** `@DataJpaTest` with Testcontainers (PostgreSQL) to verify optimistic locking and atomic inventory decrements.
- **Event Tests:** Verifying Outbox payloads are correctly inserted in the same transaction.
- **Data Pipeline Tests:** PyTest for PySpark transformations (mocking DataFrames) to prove deduplication and GMV logic.
- **E2E Tests:** API call $\rightarrow$ DB $\rightarrow$ Outbox $\rightarrow$ Kafka $\rightarrow$ Lakehouse.

## 14. Implementation Phases

| Phase | Component | Status |
|---|---|---|
| 0 | Foundation & Infrastructure | COMPLETED |
| 1 | User & Authentication | NEXT |
| 2 | Catalog | PLANNED |
| 3 | Cart | PLANNED |
| 4 | Order | PLANNED |
| 5 | Payment | PLANNED |
| 6 | Transactional Outbox / Kafka | COMPLETED |
| 7 | Bronze Pipeline | COMPLETED |
| 8 | Silver Pipeline | COMPLETED |
| 9 | Gold + DuckDB | COMPLETED |
| 10 | Airflow Local Orchestration | COMPLETED* |
| 11 | Real Application E2E | PLANNED |
| 12 | Final Local Validation | PLANNED |

*Note on COMPLETED\**: Airflow currently validates the Data Pipeline using synthetic events rather than a complete Spring Boot application flow. Data Pipeline E2E is operational, but Full Application E2E is NOT yet complete.

## 15. Completed Work Reconciliation
TASK 01–06 established the repository foundation and infrastructure.
TASK 07–12 were implemented during project execution but were not fully represented as detailed TASK entries in the original implementation plan. They are now recorded here as completed implementation milestones:
- Outbox domain model, repository, service, poller, and Kafka publisher (Phase 6).
- Bronze, Silver, and Gold pipelines (Phase 7-9).
- DuckDB analytical views and validations (Phase 9).
- Airflow local E2E orchestration (Phase 10).

*Important:* Outbox was implemented BEFORE the remaining business domains. This is an intentional implementation-order deviation rather than pretending the old phase ordering was followed exactly.

## 16. Remaining Implementation Milestones (Tasks 13-19)

### Backend Phase A — User & Authentication
**TASK 13 — User & Authentication**
Scope includes:
- user domain
- user persistence
- password handling
- authentication API
- authorization foundation
- validation
- tests
- ArchUnit boundary compliance

### Backend Phase B — Catalog
**TASK 14 — Catalog**
Scope includes:
- categories
- products
- inventory read/update foundation
- catalog APIs
- validation
- tests

### Backend Phase C — Cart
**TASK 15 — Cart**
Scope includes:
- carts
- cart items
- add/update/remove items
- cart totals
- validation
- tests

### Backend Phase D — Order
**TASK 16 — Order**
Scope includes:
- order creation
- order items
- order lifecycle
- inventory interaction
- optimistic locking
- transactional boundaries
- domain events/outbox integration
- tests

### Backend Phase E — Payment
**TASK 17 — Payment**
Scope includes:
- payment creation
- payment status
- payment failure handling
- order/payment relationship
- transactional behavior
- tests

### Integration Phase
**TASK 18 — Real Application Event Flow**
Scope: Spring Boot API $\rightarrow$ PostgreSQL transaction $\rightarrow$ outbox_events $\rightarrow$ Outbox Poller $\rightarrow$ Kafka $\rightarrow$ Bronze $\rightarrow$ Silver $\rightarrow$ Gold
This task should replace the current mock-only application side of E2E. The existing synthetic generator must remain available for independent pipeline tests.

### Final Validation Phase
**TASK 19 — Full Local E2E Validation**
Scope: API $\rightarrow$ PostgreSQL $\rightarrow$ Outbox $\rightarrow$ Kafka $\rightarrow$ Bronze $\rightarrow$ Silver $\rightarrow$ Gold $\rightarrow$ DuckDB $\rightarrow$ Data Quality Validation
Includes: reproducible local run, data quality checks, event counts, order/payment consistency, resource profiling, documentation. Do NOT introduce cloud deployment.
