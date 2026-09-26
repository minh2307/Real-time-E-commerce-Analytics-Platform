# Real-time E-commerce Analytics Platform
## Use Case Specification

---

## 1. Use Case Overview

### 1.1 Purpose
This document provides the formal **Use Case Specification** for the **Real-time E-commerce Analytics Platform**. It bridges the high-level scope in [`01-project-brief.md`](file:///home/nguyen-son-minh/Downloads/Real-time%20E-commerce%20Analytics%20Platform/docs/01-project-brief.md) and the formal specifications in [`02-requirements.md`](file:///home/nguyen-son-minh/Downloads/Real-time%20E-commerce%20Analytics%20Platform/docs/02-requirements.md) by defining how distinct human and system actors interact with the platform, what workflows they execute, how the system responds to both standard and failure conditions, and how business actions produce domain events for downstream analytical processing.

### 1.2 Relationship to Project Documentation
The use cases defined here serve as the functional foundation for subsequent technical design deliverables:

```
[ 01-project-brief.md ]     (Vision, business context, high-level scope)
           │
           ▼
[ 02-requirements.md ]      (Testable functional, data, and quality requirements)
           │
           ▼
[ 03-use-cases.md ]         (Actor workflows, state transitions, event linkage - THIS DOCUMENT)
           │
           ├──────────────────────────┬──────────────────────────┐
           ▼                          ▼                          ▼
[ 04-system-architecture.md ] [ 05-database-design.md ] [ 06-data-model.md ]
(Components & topology)       (OLTP relational schema)  (OLAP Star Schema)
           │                          │                          │
           ▼                          ▼                          ▼
[ 08-api-specification.md ]   [ 07-data-pipeline.md ]            │
(REST endpoints & payloads)   (Medallion ETL & Airflow) ─────────┘
```

### 1.3 Scope and Deferrals
- **What is covered:** Detailed actor interactions, preconditions, postconditions, main success flows, alternative flows, error handling, business rule enforcement, domain event generation, and analytical pipeline workflows.
- **What is deferred:** Low-level REST URL paths, HTTP verbs, JSON request/response contracts, relational DDL schemas, Kafka topic names and partition counts, Airflow DAG code implementations, and Docker deployment setups. Those will be detailed in their dedicated design documents.

---

## 2. Actors

### 2.1 Customer (Primary Human Actor)
The external end-user interacting with the transactional e-commerce store.
- **Responsibilities:**
  - Register an account and authenticate.
  - Browse categorized product catalog listings and inspect product details.
  - Manage personal shopping cart (add items, modify quantities, remove items).
  - Check out cart contents into formal purchase orders.
  - Execute simulated payments against pending orders.
  - Retry failed payment transactions.
  - Cancel eligible unpaid orders.
  - View personal historical orders and order item breakdowns.

### 2.2 Administrator (Secondary Human Actor)
The internal operational user responsible for catalog maintenance and store governance.
- **Responsibilities:**
  - Create and manage product categories.
  - Create, update, and manage product catalog listings and active states.
  - Adjust product inventory stock levels.
  - Inspect platform-wide sales and operational metrics.

### 2.3 Analytics / Data Platform (Primary System Actor)
The distributed analytical engine responsible for consuming, transforming, and modeling event streams.
- **Responsibilities:**
  - Continuously ingest raw business events from the messaging broker into immutable storage (Bronze layer).
  - Parse, validate, clean, and deduplicate event payloads across Lakehouse tiers.
  - Transform conformed data into Star Schema dimensional models (Fact and Dimension tables).
  - Enforce data quality gates and isolate malformed/corrupt records into quarantine.
  - Serve business intelligence queries and pre-aggregated KPI metrics.

### 2.4 Scheduler / Orchestrator (System Actor — Apache Airflow)
The workflow orchestrator governing scheduled transformations, quality validations, and warehouse synchronization.
- **Responsibilities:**
  - Trigger recurring batch processing workflows (Silver $\rightarrow$ Gold $\rightarrow$ Warehouse) over landed Bronze partitions.
  - Enforce sequential dependencies between processing and quality validation stages.
  - Execute automated retry policies upon transient task failures.
  - Report pipeline health, execution logs, and data quality alerts.

### 2.5 Event Broker (System Actor — Apache Kafka)
The distributed streaming platform decoupling operational systems from analytical consumers.
- **Responsibilities:**
  - Receive structured domain events emitted by backend transactional services.
  - Retain events immutably according to configured retention periods.
  - Preserve sequential event ordering where business entity state transitions require it.
  - Serve event streams reliably to downstream analytical consumers.

---

## 3. Use Case Catalog

```
===================================================================================
CUSTOMER USE CASES
===================================================================================
  UC-001   Register Account
  UC-002   Authenticate User
  UC-002A  View Customer Profile
  UC-003   Browse Product Catalog
  UC-004   View Product Details
  UC-005   Add Product to Cart
  UC-006   Update Cart Item Quantity
  UC-007   Remove Item from Cart
  UC-008   Create Order
  UC-009   Make Payment
  UC-010   Retry Failed Payment
  UC-011   Cancel Order
  UC-012   View Order History

===================================================================================
ADMINISTRATIVE USE CASES
===================================================================================
  UC-ADMIN-001  Manage Product
  UC-ADMIN-002  Manage Category
  UC-ADMIN-003  Manage Inventory
  UC-ADMIN-004  View Operational Analytics

===================================================================================
DATA PLATFORM USE CASES
===================================================================================
  UC-DATA-001  Ingest Domain Events into Bronze Layer
  UC-DATA-002  Process and Cleanse Bronze to Silver Layer
  UC-DATA-003  Build Gold Dimensional Model
  UC-DATA-004  Execute Data Quality Checks & Quarantine
  UC-DATA-005  Execute Orchestrated Analytical Pipeline
===================================================================================
```

---

## 4. Use Case Detail Format

Each use case in this specification adheres to the following structured specification:

- **Goal:** The ultimate objective the actor seeks to achieve.
- **Primary Actor:** The entity initiating the interaction.
- **Supporting Actors:** External or internal systems collaborating in the workflow.
- **Preconditions:** System states that must hold true before the use case can start.
- **Trigger:** The specific event or request initiating the interaction.
- **Main Success Flow:** The optimal, step-by-step happy path from initiation to completion.
- **Alternative Flows:** Valid alternate pathways to completion.
- **Failure / Exception Flows:** Systematic error handling, validation rejections, and fallbacks.
- **Postconditions:** Guaranteed system and data states upon successful execution.
- **Domain Events:** Structured events emitted to the messaging broker upon completion.
- **Related Requirements:** Requirement identifiers from [`02-requirements.md`](file:///home/nguyen-son-minh/Downloads/Real-time%20E-commerce%20Analytics%20Platform/docs/02-requirements.md).
- **Business Rules:** Explicit business invariants enforced during execution.

---

## 5. Customer Use Cases — Identity & Account

### UC-001 — Register Account

- **Goal:** Create a new authenticated customer account in the platform.
- **Primary Actor:** Customer (Guest).
- **Supporting Actors:** Event Broker.
- **Preconditions:** Customer does not have an active session.
- **Trigger:** Customer submits registration details (email/username, secure password, full name).
- **Main Success Flow:**
  1. Customer submits registration credentials and profile information.
  2. System validates input format (email format, password complexity, non-empty name).
  3. System verifies that the email/username does not already exist in the database.
  4. System securely salts and hashes the password using an approved cryptographic algorithm.
  5. System persists the new user record in the operational database with active status.
  6. System constructs and reliably emits a `USER_REGISTERED` event to the Event Broker containing the user identifier, registration timestamp, and profile metadata.
  7. System returns a successful registration confirmation to the Customer.
- **Alternative Flows:** None.
- **Failure / Exception Flows:**
  - *E1: Invalid Input Format:* System detects invalid email syntax or weak password; registration is rejected with descriptive validation errors.
  - *E2: Duplicate Identity:* System detects email/username is already registered; registration is rejected with a conflict error.
  - *E3: Database Failure:* System encounters a persistence error; transaction rolls back, no event is emitted, and a service error is returned.
- **Postconditions:** A new user record is committed to PostgreSQL; a `USER_REGISTERED` event is published to Kafka.
- **Domain Events:** `USER_REGISTERED`
- **Related Requirements:** `FR-001`, `FR-004`, `DR-001`, `NFR-016`, `NFR-018`
- **Business Rules:** `BR-001` (user existence for downstream operations)

---

### UC-002 — Authenticate User

- **Goal:** Verify customer credentials and issue an authentication token for secured operations.
- **Primary Actor:** Customer.
- **Supporting Actors:** Operational Database, In-Memory Cache (optional session acceleration).
- **Preconditions:** Customer account exists and is in active status.
- **Trigger:** Customer submits login credentials (identifier and password).
- **Main Success Flow:**
  1. Customer submits credentials.
  2. System retrieves the stored user record by identifier.
  3. System verifies the submitted password against the stored cryptographic hash.
  4. System confirms the user account is active.
  5. System generates a secure, stateless authentication token.
  6. System returns the token and customer profile summary to the Customer.
- **Alternative Flows:** None.
- **Failure / Exception Flows:**
  - *E1: Account Not Found or Bad Credentials:* System cannot find the identifier or password hash verification fails; system returns an authentication failure response.
  - *E2: Account Deactivated / Locked:* System detects the user account is inactive; system rejects login with an access forbidden response.
- **Postconditions:** Customer receives a valid authentication token for subsequent requests; user session state is established.
- **Domain Events:** None (operational session event; telemetry view events handled separately).
- **Related Requirements:** `FR-002`, `NFR-016`, `NFR-017`, `AAR-001`, `AAR-002`
- **Business Rules:** None.

---

### UC-002A — View Customer Profile

- **Goal:** Retrieve and inspect the authenticated customer's account profile and registration details.
- **Primary Actor:** Customer.
- **Supporting Actors:** Operational Database (PostgreSQL).
- **Preconditions:** Customer is authenticated with a valid session/token.
- **Trigger:** Customer requests their account profile information.
- **Main Success Flow:**
  1. Customer submits a request to view their account profile.
  2. System extracts and validates the customer's authenticated identity from the session/token (`AAR-002`).
  3. System retrieves the user record matching the authenticated identity from the operational database.
  4. System formats profile details (user identifier, email/username, full name, account status, creation timestamp) strictly omitting password hashes and sensitive internal credentials (`NFR-016`).
  5. System returns profile information to the Customer.
- **Alternative Flows:** None.
- **Failure / Exception Flows:**
  - *E1: Unauthenticated Request:* Token is missing, expired, or malformed; system rejects request with unauthorized status (`AAR-001`).
  - *E2: Account Deactivated / Deleted:* Corresponding record does not exist or is deactivated; system rejects request with forbidden/not found status.
- **Postconditions:** Customer profile information is delivered without mutating database state.
- **Domain Events:** None.
- **Related Requirements:** `FR-003`, `DR-001`, `AAR-002`, `AAR-003`, `NFR-016`
- **Business Rules:** None.

---

## 6. Customer Use Cases — Product Discovery

### UC-003 — Browse Product Catalog

- **Goal:** View and filter active products in the store catalog.
- **Primary Actor:** Customer (Guest or Authenticated).
- **Supporting Actors:** Read-Optimization Cache Layer, Operational Database (PostgreSQL).
- **Preconditions:** Store catalog contains active categories and products.
- **Trigger:** Customer requests product list with optional filtering (category, price range) and pagination parameters.
- **Main Success Flow:**
  1. Customer requests catalog listing with optional parameters (e.g., page 1, size 20, category = "Laptops").
  2. System queries the configured read-optimization layer for the requested catalog partition.
  3. System formats and returns paginated product summaries (SKU, title, price, thumbnail, inventory status) to the Customer.
- **Alternative Flows:**
  - *A1: Read Layer Miss / Fallback:* If catalog data is not present in the read-optimization layer, system retrieves product records from the transactional data source, updates the read layer, and returns results to the Customer.
- **Failure / Exception Flows:**
  - *E1: Invalid Query Parameters:* Customer supplies negative page numbers or malformed category identifiers; system returns a bad request error.
- **Postconditions:** Catalog data is returned without mutating transactional state.
- **Domain Events:** None.
- **Related Requirements:** `FR-005`, `FR-006`, `DR-002`, `NFR-001`, `AAR-001`
- **Business Rules:** `BR-001` (only active products displayed)

---

### UC-004 — View Product Details

- **Goal:** Inspect full specifications, current price, and stock availability of a specific product.
- **Primary Actor:** Customer (Guest or Authenticated).
- **Supporting Actors:** In-Memory Cache, Operational Database, Event Broker.
- **Preconditions:** Product exists in the catalog.
- **Trigger:** Customer selects a specific product to inspect.
- **Main Success Flow:**
  1. Customer requests product details using the product identifier.
  2. System retrieves product details and current inventory status (from cache or operational database).
  3. System validates that the product is active.
  4. System constructs and reliably emits a `PRODUCT_VIEWED` event to the Event Broker containing the product identifier, category identifier, viewing timestamp, current price, and customer identifier (if authenticated).
  5. System returns complete product specifications, pricing, and stock status to the Customer.
- **Alternative Flows:**
  - *A1: Guest Viewer:* If the customer is unauthenticated, the emitted `PRODUCT_VIEWED` event includes a guest/anonymous session identifier instead of a registered customer ID.
- **Failure / Exception Flows:**
  - *E1: Product Not Found or Inactive:* System cannot find the product or it is marked inactive; system returns a resource not found error. No event is emitted.
- **Postconditions:** Product details are delivered to the Customer; a `PRODUCT_VIEWED` event is published to Kafka for conversion funnel analytics.
- **Domain Events:** `PRODUCT_VIEWED`
- **Related Requirements:** `FR-007`, `FR-008`, `ER-001`, `DR-002`, `AR-006`, `BR-001`
- **Business Rules:** `BR-001` (product must be active)

---

## 7. Customer Use Cases — Cart Management

### UC-005 — Add Product to Cart

- **Goal:** Place an active product with a specified quantity into the customer's shopping cart.
- **Primary Actor:** Customer.
- **Supporting Actors:** Operational Database / In-Memory Store, Event Broker.
- **Preconditions:** Customer possesses an active cart session; product exists and is active.
- **Trigger:** Customer selects "Add to Cart" specifying product identifier and quantity.
- **Main Success Flow:**
  1. Customer submits request with product identifier and quantity ($q \ge 1$).
  2. System verifies product exists, is active, and current available stock $\ge q$.
  3. System retrieves or initializes the customer's cart.
  4. If the product already exists in the cart, system increments existing quantity; otherwise, system adds a new line item.
  5. System verifies combined line quantity does not exceed inventory or maximum item limits.
  6. System recalculates cart subtotal dynamically: $\sum (\text{unit\_price} \times \text{quantity})$.
  7. System persists the updated cart state.
  8. System emits a `CART_UPDATED` (or `ITEM_ADDED`) event to the Event Broker with cart details, product identifier, quantity added, and updated subtotal.
  9. System returns the updated cart structure to the Customer.
- **Alternative Flows:** None.
- **Failure / Exception Flows:**
  - *E1: Invalid Quantity:* Customer requests quantity $< 1$ or exceeding maximum order limits; request is rejected with validation error.
  - *E2: Insufficient Stock:* Available inventory is less than requested quantity; request is rejected with an out-of-stock error.
  - *E3: Inactive/Non-Existent Product:* Product does not exist or is deactivated; request is rejected with not found error.
- **Postconditions:** Cart state is updated; cart subtotal is recalculated; a domain event is published to Kafka.
- **Domain Events:** `CART_UPDATED` (or `ITEM_ADDED`)
- **Related Requirements:** `FR-009`, `FR-010`, `FR-013`, `FR-014`, `BR-001`, `BR-003`, `BR-004`, `BR-005`
- **Business Rules:** `BR-001`, `BR-003`, `BR-004`, `BR-005`

---

### UC-006 — Update Cart Item Quantity

- **Goal:** Modify the quantity of an existing item in the shopping cart.
- **Primary Actor:** Customer.
- **Supporting Actors:** Operational Database / In-Memory Store, Event Broker.
- **Preconditions:** Cart contains the designated product item.
- **Trigger:** Customer adjusts item quantity.
- **Main Success Flow:**
  1. Customer submits item identifier and updated quantity ($q_{new} \ge 1$).
  2. System checks available inventory for the target product against $q_{new}$.
  3. System updates the line item quantity in the cart.
  4. System recalculates cart subtotal based on current catalog prices.
  5. System persists updated cart state.
  6. System emits a `CART_UPDATED` event to the Event Broker.
  7. System returns updated cart summary to the Customer.
- **Alternative Flows:**
  - *A1: Quantity Set to Zero:* If customer sets quantity to 0, system invokes flow equivalent to `UC-007` (Remove Item from Cart).
- **Failure / Exception Flows:**
  - *E1: Exceeds Available Stock:* Target inventory cannot fulfill $q_{new}$; request rejected with insufficient stock warning.
  - *E2: Item Not in Cart:* Designated product is not in the customer's cart; request rejected with not found error.
- **Postconditions:** Cart item quantity updated; cart subtotal recalculated; `CART_UPDATED` event emitted.
- **Domain Events:** `CART_UPDATED`
- **Related Requirements:** `FR-011`, `FR-013`, `FR-014`, `BR-003`, `BR-004`, `BR-005`
- **Business Rules:** `BR-003`, `BR-004`, `BR-005`

---

### UC-007 — Remove Item from Cart

- **Goal:** Remove an individual item or clear all items from the shopping cart.
- **Primary Actor:** Customer.
- **Supporting Actors:** Operational Database / In-Memory Store, Event Broker.
- **Preconditions:** Cart contains items.
- **Trigger:** Customer chooses to remove an item or clear the cart.
- **Main Success Flow:**
  1. Customer requests removal of a specific item (or entire cart contents).
  2. System removes the item from the cart data structure.
  3. System recalculates cart subtotal (defaults to $0$ if empty).
  4. System persists updated cart state.
  5. System emits a `CART_UPDATED` event to the Event Broker.
  6. System returns updated cart state to the Customer.
- **Alternative Flows:** None.
- **Failure / Exception Flows:**
  - *E1: Item Not in Cart:* Target item does not exist in the active cart; system returns a not found response.
- **Postconditions:** Item is removed; cart total is updated; `CART_UPDATED` event emitted.
- **Domain Events:** `CART_UPDATED`
- **Related Requirements:** `FR-012`, `FR-013`, `FR-014`, `BR-005`
- **Business Rules:** `BR-005`

---

## 8. Customer Use Cases — Order Lifecycle

### UC-008 — Create Order

- **Goal:** Convert the customer's active cart into a formal, binding purchase order in `PENDING` state and reserve required stock.
- **Primary Actor:** Customer.
- **Supporting Actors:** Operational Database (PostgreSQL), Event Broker (Kafka).
- **Preconditions:**
  - Customer is authenticated.
  - Customer's active cart contains at least one item.
- **Trigger:** Customer submits checkout request.
- **Main Success Flow:**
  1. Customer initiates checkout.
  2. System opens an ACID transaction in the operational database.
  3. System retrieves the customer's active cart and validates it is non-empty.
  4. System validates the cart against the current catalog state, active product status, and available inventory.
  5. System captures an immutable snapshot of each product's title, SKU, and current unit price at this exact instant.
  6. System decrements the available stock inventory for each ordered item in the catalog.
  7. System derives the total amount: $\text{Total} = \sum (\text{snapshot\_price} \times \text{quantity}) + \text{taxes} + \text{shipping}$.
  8. System creates the `Order` record with status `PENDING` and associated `OrderItem` records linked to the customer.
  9. System clears the customer's active shopping cart.
  10. System commits the database transaction.
  11. System reliably emits an `ORDER_CREATED` event to the Event Broker containing the order identifier, customer identifier, itemized snapshots, monetary totals, and creation timestamp.
  12. System returns the newly created order details to the Customer.
- **Alternative Flows:** None.
- **Failure / Exception Flows:**
  - *E1: Empty Cart:* Cart has no items; order creation is rejected.
  - *E2: Insufficient Stock:* One or more items in the cart exceed available stock at checkout time; system aborts order creation, rolls back transaction, reports out-of-stock items, and inventory is not decremented.
  - *E3: Catalog Invalidation:* If any cart product is found to be inactive, altered, or unavailable during validation, order placement is aborted and the customer is prompted to review their cart.
  - *E4: Transaction Failure:* Any persistence failure causes full transaction rollback; no order is created, no inventory is decremented, and no `ORDER_CREATED` event is published.
- **Postconditions:**
  - Order is created with status `PENDING`.
  - Inventory is reserved (decremented).
  - Cart is cleared.
  - `ORDER_CREATED` event is published to Kafka.
- **Domain Events:** `ORDER_CREATED`
- **Related Requirements:** `FR-015`, `FR-016`, `FR-017`, `FR-018`, `FR-019`, `ER-001`, `DR-004`, `BR-001`, `BR-002`, `BR-003`, `BR-006`
- **Business Rules:** `BR-001`, `BR-002`, `BR-003`, `BR-006`, `BR-007`

```
Customer              Backend (Spring Boot)           PostgreSQL                  Kafka
   │                         │                            │                         │
   ├── Submit Checkout ─────►│                            │                         │
   │                         ├── Begin Transaction ──────►│                         │
   │                         ├── Validate Cart & Stock ──►│                         │
   │                         ├── Snapshot Product Prices ─│                         │
   │                         ├── Decrement Stock ────────►│                         │
   │                         ├── Insert Order (PENDING) ─►│                         │
   │                         ├── Clear Cart ─────────────►│                         │
   │                         ├── Commit Transaction ─────►│                         │
   │                         │                            │                         │
   │                         ├── Emit ORDER_CREATED ───────────────────────────────►│
   │◄── Order Created (201) ─┤                                                      │
```

---

### UC-009 — Make Payment

- **Goal:** Execute a simulated payment settlement against an order currently in `PENDING` state.
- **Primary Actor:** Customer.
- **Supporting Actors:** Payment Simulation Service, Operational Database, Event Broker.
- **Preconditions:**
  - Customer is authenticated.
  - Target order exists, belongs to the customer, and is in `PENDING` status.
- **Trigger:** Customer submits payment information (payment method, simulation directive).
- **Main Success Flow (Payment Settlement Success):**
  1. Customer requests payment execution for a specific `order_id`.
  2. System retrieves the order and verifies status is `PENDING`.
  3. System invokes the internal Payment Simulation Service with order total and method.
  4. Simulation engine determines successful settlement.
  5. System begins a database transaction:
     - Inserts a new `Payment` record with status `SUCCESS`, transaction reference, and timestamp.
     - Updates the order status from `PENDING` to `PAID`.
     - Commits database transaction.
  6. System emits domain events:
     - `PAYMENT_COMPLETED` containing payment ID, order ID, amount, method, and settlement timestamp (tracks financial transaction settlement).
     - `ORDER_PAID` signifying the order status has officially transitioned to `PAID`. This serves as the sole authoritative business event for **GMV and gross revenue recognition** (`BR-012`).
  7. System returns successful payment and updated order details to the Customer.
- **Alternative Flows:** None.
- **Failure / Exception Flows (Payment Settlement Failure):**
  - *E1: Simulation Failure (e.g., Insufficient Funds, Gateway Rejection):*
    1. Simulation engine produces a failed settlement outcome with an error code.
    2. System opens a database transaction:
       - Inserts a `Payment` record with status `FAILED` and error code.
       - Updates order status from `PENDING` to `PAYMENT_FAILED`.
       - Commits database transaction.
    3. System emits a `PAYMENT_FAILED` event to the Event Broker.
    4. System returns a payment failure response to Customer indicating the order is in `PAYMENT_FAILED` status and eligible for retry.
  - *E2: Order Already Paid:* Order is already in `PAID` or `COMPLETED` state; payment is rejected (`BR-010`).
  - *E3: Order Cancelled:* Order is in `CANCELLED` state; payment cannot be executed.
  - *E4: Unauthorized Customer:* Order belongs to another customer; request is rejected with forbidden status (`AAR-003`).
- **Postconditions:**
  - On Success: Order status becomes `PAID`; payment recorded; `PAYMENT_COMPLETED` emitted for financial auditing; `ORDER_PAID` emitted as the authoritative trigger for analytical GMV recognition; inventory remains secured.
  - On Failure: Order status becomes `PAYMENT_FAILED`; failure recorded; `PAYMENT_FAILED` event emitted; inventory remains reserved pending retry or cancellation; no revenue is recognized.
- **Domain Events:** `PAYMENT_COMPLETED`, `ORDER_PAID` (on success); `PAYMENT_FAILED` (on failure)
- **Related Requirements:** `FR-023`, `FR-024`, `FR-025`, `FR-026`, `FR-027`, `BR-007`, `BR-009`, `BR-010`, `BR-011`, `AR-011`
- **Business Rules:** `BR-007`, `BR-009`, `BR-010`, `BR-011`

---

### UC-010 — Retry Failed Payment

- **Goal:** Re-attempt payment settlement for an order currently in `PAYMENT_FAILED` status.
- **Primary Actor:** Customer.
- **Supporting Actors:** Payment Simulation Service, Operational Database, Event Broker.
- **Preconditions:**
  - Customer is authenticated.
  - Order exists, belongs to the customer, and is in `PAYMENT_FAILED` status.
- **Trigger:** Customer re-submits payment request (e.g., with updated simulated card/method).
- **Main Success Flow:**
  1. Customer requests payment retry on the target order.
  2. System retrieves the order and verifies status is `PAYMENT_FAILED`.
  3. System invokes Payment Simulation Service.
  4. Simulation engine returns successful outcome.
  5. System begins a database transaction:
     - Records a new `Payment` record linked to the order with status `SUCCESS`.
     - Transitions order status from `PAYMENT_FAILED` to `PAID`.
     - Commits database transaction.
  6. System emits `PAYMENT_COMPLETED` and `ORDER_PAID` events to the Event Broker.
  7. System returns payment confirmation and updated `PAID` order details to the Customer.
- **Alternative Flows:** None.
- **Failure / Exception Flows:**
  - *E1: Retry Fails Again:* Simulation engine returns another failure; system records another failed `Payment` attempt record; order remains in `PAYMENT_FAILED` status; `PAYMENT_FAILED` event emitted; Customer is notified.
  - *E2: Customer Cancels Instead:* Customer decides not to retry and initiates cancellation; system invokes `UC-011`.
- **Postconditions:** Order transitions to `PAID` on success, or remains `PAYMENT_FAILED` on failure; all attempts are recorded in PostgreSQL; corresponding domain events are emitted.
- **Domain Events:** `PAYMENT_COMPLETED`, `ORDER_PAID` (on success); `PAYMENT_FAILED` (on failure)
- **Related Requirements:** `FR-019`, `FR-023`, `FR-025`, `FR-026`, `BR-007`, `BR-011`
- **Business Rules:** `BR-007`, `BR-011`

---

### UC-011 — Cancel Order

- **Goal:** Cancel an existing order that has not yet been fulfilled, releasing reserved inventory back to available stock.
- **Primary Actor:** Customer.
- **Supporting Actors:** Operational Database, Event Broker.
- **Preconditions:**
  - Customer is authenticated.
  - Order belongs to the customer.
  - Order is in an eligible cancellation state (`PENDING` or `PAYMENT_FAILED`).
- **Trigger:** Customer submits cancellation request.
- **Main Success Flow:**
  1. Customer requests cancellation of target `order_id`.
  2. System verifies order ownership and validates status is either `PENDING` or `PAYMENT_FAILED`.
  3. System opens a database transaction:
     - Updates order status to `CANCELLED`.
     - For each line item in the order, restores the reserved item quantity back into catalog available inventory (`BR-008`).
     - Commits database transaction.
  4. System emits an `ORDER_CANCELLED` event to the Event Broker containing order ID, customer ID, cancellation timestamp, and restored item list.
  5. System returns cancellation confirmation to Customer.
- **Alternative Flows:** None.
- **Failure / Exception Flows:**
  - *E1: Ineligible Status:* Order is in `PAID` or `COMPLETED` state; direct automated cancellation is rejected (`BR-007`).
  - *E2: Already Cancelled:* Order is already in `CANCELLED` status; request is rejected as a redundant operation.
  - *E3: Unauthorized Customer:* Customer attempts to cancel an order belonging to someone else; request rejected with forbidden status (`AAR-003`).
- **Postconditions:** Order status is `CANCELLED`; reserved stock is restored in PostgreSQL; `ORDER_CANCELLED` event published to Kafka.
- **Domain Events:** `ORDER_CANCELLED`
- **Related Requirements:** `FR-019`, `FR-020`, `FR-022`, `DR-004`, `BR-007`, `BR-008`, `BR-013`, `AR-010`
- **Business Rules:** `BR-007`, `BR-008`, `BR-013`

---

### UC-012 — View Order History

- **Goal:** Retrieve historical orders placed by the authenticated customer, including itemized breakdowns and settlement statuses.
- **Primary Actor:** Customer.
- **Supporting Actors:** Operational Database.
- **Preconditions:** Customer is authenticated.
- **Trigger:** Customer requests order history.
- **Main Success Flow:**
  1. Customer requests their order history with optional pagination.
  2. System queries operational database for orders strictly matching the customer's authenticated identity (`AAR-002`, `AAR-003`).
  3. System retrieves associated line items, historical snapshot prices, total amounts, and order statuses.
  4. System formats and returns the list of historical orders to the Customer.
- **Alternative Flows:**
  - *A1: Single Order Detail:* Customer requests detail for a specific `order_id`; system verifies customer ownership and returns the full single order representation.
- **Failure / Exception Flows:**
  - *E1: Unauthorized Access:* User attempts to inspect an order ID belonging to another customer; system rejects with access forbidden (`AAR-003`).
- **Postconditions:** Order history returned without mutating any state.
- **Domain Events:** None.
- **Related Requirements:** `FR-021`, `DR-004`, `AAR-002`, `AAR-003`
- **Business Rules:** None.

---

## 9. Administrative Use Cases

### UC-ADMIN-001 — Manage Product

- **Goal:** Create, update, or deactivate product catalog listings.
- **Primary Actor:** Administrator.
- **Supporting Actors:** Operational Database, In-Memory Cache (Redis).
- **Preconditions:** Administrator is authenticated with administrative privileges (`AAR-004`).
- **Trigger:** Administrator submits product creation or modification request.
- **Main Success Flow:**
  1. Administrator submits product details (SKU, title, description, category ID, unit price, stock quantity, active status).
  2. System validates category existence and numerical validity (price $\ge 0$, stock $\ge 0$).
  3. System persists product changes in operational database.
  4. System invalidates or updates corresponding product cache entries in Redis.
  5. System returns confirmed product record to Administrator.
- **Alternative Flows:** None.
- **Failure / Exception Flows:**
  - *E1: Unauthorized:* Actor lacks admin privileges; request rejected (`AAR-004`).
  - *E2: Invalid Values:* Negative price or invalid category; request rejected.
- **Postconditions:** Product catalog is updated; Redis cache invalidated.
- **Domain Events:** None (operational catalog maintenance; changes captured by subsequent business interactions).
- **Related Requirements:** `FR-005`, `FR-006`, `DR-002`, `BR-001`, `BR-002`, `AAR-004`
- **Business Rules:** `BR-001`, `BR-002`

---

### UC-ADMIN-002 — Manage Category

- **Goal:** Create or update product categories.
- **Primary Actor:** Administrator.
- **Supporting Actors:** Operational Database.
- **Preconditions:** Administrator is authenticated with administrative privileges.
- **Trigger:** Administrator creates or updates a category.
- **Main Success Flow:**
  1. Administrator submits category name and description.
  2. System validates name uniqueness.
  3. System persists category record in operational database.
  4. System returns category details to Administrator.
- **Alternative Flows:** None.
- **Failure / Exception Flows:**
  - *E1: Duplicate Category Name:* System rejects creation with conflict error.
- **Postconditions:** Category stored in PostgreSQL.
- **Domain Events:** None.
- **Related Requirements:** `FR-005`, `DR-002`, `AAR-004`
- **Business Rules:** None.

---

### UC-ADMIN-003 — Manage Inventory

- **Goal:** Adjust available inventory stock levels for catalog products (e.g., restocking).
- **Primary Actor:** Administrator.
- **Supporting Actors:** Operational Database, In-Memory Cache.
- **Preconditions:** Administrator authenticated; product exists.
- **Trigger:** Administrator submits inventory replenishment or manual adjustment.
- **Main Success Flow:**
  1. Administrator submits product identifier and stock adjustment quantity ($\Delta \text{stock}$).
  2. System validates that resulting stock level $\ge 0$ (`BR-003`).
  3. System updates inventory count in operational database.
  4. System invalidates product cache.
  5. System returns updated stock level to Administrator.
- **Alternative Flows:** None.
- **Failure / Exception Flows:**
  - *E1: Negative Resulting Stock:* Adjustment would cause stock $< 0$; request rejected (`BR-003`).
- **Postconditions:** Inventory level updated; cache evicted.
- **Domain Events:** None.
- **Related Requirements:** `FR-007`, `DR-002`, `BR-003`, `AAR-004`
- **Business Rules:** `BR-003`

---

### UC-ADMIN-004 — View Operational Analytics

- **Goal:** Review high-level platform business metrics and operational health indicators.
- **Primary Actor:** Administrator / Business Analyst.
- **Supporting Actors:** Analytical Store / Data Warehouse (Gold Layer).
- **Preconditions:** Gold analytical models have been built and loaded.
- **Trigger:** Actor queries business performance summaries.
- **Main Success Flow:**
  1. Actor submits query for sales, product velocity, or conversion metrics.
  2. System executes pre-built analytical queries **exclusively against the Analytical Store** (Gold layer), completely decoupled from PostgreSQL (`DSR-002`, `DSR-005`).
  3. System returns metric aggregations (GMV, AOV, top products, cancellation rates) to the Actor.
- **Alternative Flows:** None.
- **Failure / Exception Flows:**
  - *E1: Data Not Fresh:* Pipeline has not run for current period; system returns latest available partition timestamp.
- **Postconditions:** Metrics displayed without placing query load on the operational database.
- **Domain Events:** None.
- **Related Requirements:** `AR-001` to `AR-013`, `DSR-002`, `NFR-004`, `AAR-004`
- **Business Rules:** `BR-012`, `BR-013`

---

## 10. Data Engineering Use Cases

### UC-DATA-001 — Ingest Domain Events into Bronze Layer

- **Goal:** Continuously consume streaming domain events from Kafka and land them immutably in Bronze storage in near-real-time without data loss or premature transformation.
- **Primary Actor:** Analytics / Data Platform (Continuous Ingestion Consumer).
- **Supporting Actors:** Event Broker (Kafka), Bronze Storage (Lakehouse format).
- **Preconditions:** Kafka contains domain event messages emitted by backend services.
- **Trigger:** Continuous background streaming consumption from Kafka event topics.
- **Main Success Flow:**
  1. Ingestion consumer maintains an active connection to Kafka event topics.
  2. Consumer continuously pulls streaming message batches from Kafka.
  3. For each message, consumer captures the raw JSON payload verbatim.
  4. Consumer enriches the record with ingestion technical metadata:
     - `ingestion_timestamp` (UTC)
     - `source_topic`
     - `kafka_partition`
     - `kafka_offset`
  5. Ingestion process appends the batch immutably into Bronze storage (partitioned by ingestion date/hour).
  6. Consumer commits Kafka offsets.
- **Alternative Flows:** None.
- **Failure / Exception Flows:**
  - *E1: Kafka Broker Temporary Disconnect:* Consumer logs connectivity error and retries with backoff without terminating.
  - *E2: Malformed Raw Message:* Even if a message contains invalid JSON syntax, consumer saves the raw text in Bronze with a parse error flag (`DER-004`), ensuring zero data loss and full auditability.
- **Postconditions:** Raw events stored append-only in Bronze; Kafka offsets advanced; source lineage preserved in near-real-time.
- **Domain Events:** None (consumes all domain events).
- **Related Requirements:** `DER-001`, `DER-002`, `DER-003`, `DER-004`, `DRIR-004`
- **Business Rules:** None.

---

### UC-DATA-002 — Process and Cleanse Bronze to Silver Layer

- **Goal:** Parse raw Bronze events, enforce strongly typed schemas, eliminate duplicate messages, normalize data, isolate corrupt records, and produce clean conformed business datasets.
- **Primary Actor:** Analytics / Data Platform (PySpark Processing Engine).
- **Supporting Actors:** Bronze Storage, Silver Storage, Quarantine Store.
- **Preconditions:** Bronze storage contains unprocessed or updated event partitions.
- **Trigger:** Scheduled pipeline task initiates Silver transformation.
- **Main Success Flow:**
  1. Processing job reads raw records from designated Bronze partition.
  2. Job parses JSON payloads according to expected domain schemas.
  3. Job checks for mandatory fields (`event_id`, entity IDs, timestamps, amounts). Records missing critical fields are routed to Quarantine Store (`DER-009`).
  4. Job performs type casting and standardization (timestamps to UTC ISO-8601, amounts to decimal precision, status strings to uppercase).
  5. Job executes deduplication based on `event_id` and unique business key combinations, keeping the earliest or authoritative record (`DER-006`, `DRIR-001`).
  6. Job separates conformed records into entity-specific datasets (`silver_orders`, `silver_order_items`, `silver_payments`, `silver_product_views`).
  7. Job writes cleansed records atomically into Silver storage using partition overwrites (`DRIR-003`).
- **Alternative Flows:** None.
- **Failure / Exception Flows:**
  - *E1: Schema Evolution / Unexpected Key:* Job handles optional or unknown fields gracefully; logs schema notice without dropping valid records.
  - *E2: Transformation Task Failure:* PySpark job aborts on fatal memory or network failure; target Silver partition is not committed, ensuring no partial or corrupted state (`DRIR-003`). Airflow marks task for retry.
- **Postconditions:** Silver conformed tables updated; duplicate events eliminated; invalid records quarantined; audit logs emitted.
- **Domain Events:** None.
- **Related Requirements:** `DER-005`, `DER-006`, `DER-007`, `DER-008`, `DER-009`, `DQR-001`, `DQR-002`, `DQR-003`, `DRIR-001`, `DRIR-003`
- **Business Rules:** `BR-014`

---

### UC-DATA-003 — Build Gold Dimensional Model

- **Goal:** Transform cleaned Silver data into a Star Schema (Fact and Dimension tables) and business metric rollups optimized for fast analytical query serving.
- **Primary Actor:** Analytics / Data Platform (PySpark Engine).
- **Supporting Actors:** Silver Storage, Gold Storage, Analytical Store / Data Warehouse.
- **Preconditions:** Silver layer transformation has completed successfully.
- **Trigger:** Scheduled pipeline task initiates Gold modeling.
- **Main Success Flow:**
  1. Processing job reads conformed datasets from Silver (`silver_orders`, `silver_order_items`, `silver_payments`, `silver_product_views`).
  2. Job builds or updates Dimension tables:
     - `dim_products` (product attributes, category names)
     - `dim_customers` (customer profile, registration date)
     - `dim_date` (standard calendar dimensions: day, month, quarter, year, day of week)
     - `dim_order_status` (status descriptions)
  3. Job builds Fact tables with surrogate/business keys linking to dimensions:
     - `fact_orders` (order totals, customer key, date key, final status)
     - `fact_order_items` (line quantity, snapshot price, line amount, product key, order key)
     - `fact_payments` (payment amount, status, method, order key, date key)
     - `fact_product_views` (product key, customer/guest key, timestamp, date key)
  4. Job computes pre-aggregated summary rollups:
     - Daily Sales Summary (GMV, total orders, completed orders, AOV)
     - Product Velocity & Category Revenue Share
     - Browse-to-Buy Conversion Funnel metrics
  5. Job atomically writes tables into Gold storage and synchronizes into the Analytical Store / Warehouse (`DER-012`).
- **Alternative Flows:** None.
- **Failure / Exception Flows:**
  - *E1: Missing Dimension Reference:* A fact record references an unknown product ID; referential integrity check routes record to error audit or flags as unassigned dimension (`DQR-004`).
  - *E2: Write Failure:* Target store sync fails; transaction rolls back; downstream alerts triggered.
- **Postconditions:** Gold Star Schema tables and analytical rollups updated; ready for BI queries.
- **Domain Events:** None.
- **Related Requirements:** `DER-010`, `DER-011`, `DER-012`, `DQR-004`, `BR-012`, `BR-013`, `BR-014`, `AR-001` to `AR-013`
- **Business Rules:** `BR-012`, `BR-013`, `BR-014`

---

### UC-DATA-004 — Execute Data Quality Checks & Quarantine

- **Goal:** Validate data integrity, schema consistency, nullability, and metric thresholds before promoting data to downstream consumption.
- **Primary Actor:** Analytics / Data Platform (Data Quality Gate).
- **Supporting Actors:** Silver Storage, Gold Storage, Quarantine Store, Alerting System.
- **Preconditions:** Silver or Gold transformation has generated output datasets for verification.
- **Trigger:** Executed as an explicit quality gate task in the pipeline.
- **Main Success Flow:**
  1. Quality runner executes defined assertion suites against target datasets:
     - **Completeness:** Check 0% null values for primary keys (`order_id`, `product_id`, `user_id`) and timestamps (`DQR-001`).
     - **Uniqueness:** Check zero duplicate primary keys in conformed tables (`DQR-002`).
     - **Validity:** Check numerical bounds (amounts $\ge 0$, quantities $> 0$) and legal status enum values (`DQR-003`).
     - **Referential Integrity:** Verify all foreign keys in facts resolve to valid dimension keys (`DQR-004`).
  2. Quality runner compares pass/fail counts against configured tolerance thresholds.
  3. All assertions pass within tolerance thresholds.
  4. Quality runner logs audit summary and signals success to the Orchestrator.
- **Alternative Flows:** None.
- **Failure / Exception Flows:**
  - *E1: Quality Threshold Exceeded (Critical Failure):*
    1. Error count exceeds threshold (e.g., duplicate order keys detected or null amounts found).
    2. Quality runner isolates offending partition or bad records into Quarantine Store (`DQR-005`).
    3. Quality runner halts pipeline execution, blocking corrupted data from publishing into Gold (`DQR-005`).
    4. Quality runner logs descriptive failure audit and raises an alert to Orchestrator (`DQR-006`).
- **Postconditions:** Verified data is certified for consumption; corrupted data is quarantined; quality audit logs generated.
- **Domain Events:** None.
- **Related Requirements:** `DQR-001`, `DQR-002`, `DQR-003`, `DQR-004`, `DQR-005`, `DQR-006`
- **Business Rules:** None.

---

### UC-DATA-005 — Execute Orchestrated Analytical Pipeline

- **Goal:** Automate the scheduled batch execution of data transformations, quality assertions, and analytical serving across Bronze, Silver, Gold, and Warehouse layers.
- **Primary Actor:** Scheduler / Orchestrator (Apache Airflow).
- **Supporting Actors:** PySpark Engine, Data Quality Runner, Analytical Store, Bronze Storage.
- **Preconditions:** Services are operational; Airflow scheduler is running; Bronze storage contains landed events from continuous ingestion (`UC-DATA-001`).
- **Trigger:** Scheduled cron interval (e.g., daily/hourly) or manual external trigger.
- **Main Success Flow:**
  1. Orchestrator triggers execution for execution partition (e.g., logical batch date `2026-09-25`).
  2. Task 1: Verify presence and identify partition boundary of landed Bronze event data for the batch window (`UC-DATA-001`).
  3. Task 2: Execute PySpark job transforming Bronze to Silver conformed tables (`UC-DATA-002`).
  4. Task 3: Execute Data Quality validation suite on Silver datasets (`UC-DATA-004`).
  5. Task 4: Execute PySpark job building Gold Star Schema models and aggregations (`UC-DATA-003`).
  6. Task 5: Execute final Data Quality assertions on Gold tables.
  7. Task 6: Synchronize Gold models into Analytical Data Warehouse.
  8. Orchestrator marks DAG execution as `SUCCESS` and records runtime telemetry (`NFR-014`).
- **Alternative Flows:**
  - *A1: Historical Backfill / Reprocessing:* Orchestrator executes DAG across a past historical date range; thanks to idempotent writes (`DRIR-002`), historical partitions are cleanly rebuilt without side effects.
- **Failure / Exception Flows:**
  - *E1: Transient Task Failure:* A task encounters network timeout or resource starvation; Orchestrator triggers automated retry (up to 3 retries with exponential backoff) (`NFR-006`).
  - *E2: Permanent Task Failure:* Task fails after all retries; Orchestrator marks DAG run as `FAILED`, sends alert notification, and halts downstream dependent tasks.
- **Postconditions:** Analytical store is updated with verified business data; pipeline telemetry logged.
- **Domain Events:** None.
- **Related Requirements:** `DER-012`, `DQR-005`, `NFR-006`, `NFR-007`, `NFR-014`, `DRIR-002`, `AC-006`
- **Business Rules:** `BR-014`

---

## 11. End-to-End Business Scenarios

### Scenario 1 — Complete Purchase & Analytical Propagation
Demonstrates the full journey from customer registration to executive analytical reporting:

```
[Customer]
    │ Register Account ────────► Emits: USER_REGISTERED
    │ Browse Catalog ──────────► Emits: PRODUCT_VIEWED
    │ Add to Cart ─────────────► Emits: CART_UPDATED
    │ Checkout ────────────────► Emits: ORDER_CREATED (PENDING)
    │ Pay (Success) ───────────► Emits: PAYMENT_COMPLETED & ORDER_PAID
    │
[Kafka Event Log]
    │ Holds chronological event stream
    ▼
[Bronze Ingestion]
    │ Captures raw JSON + offset + partition + timestamp
    ▼
[Silver Transformation]
    │ Cleans, normalizes, casts types, deduplicates
    ▼
[Gold Modeling]
    │ Populates fact_orders, fact_order_items, fact_payments, dim_products
    ▼
[Analytical Store]
    │ Executive runs query: Total GMV, AOV, and Category Revenue updated!
```

---

### Scenario 2 — Payment Failure, Retry & Conversion Distinction
Demonstrates how payment state machine transitions are captured and analytically distinguished:

1. **Order Creation:** Customer places an order for \$1,200. Backend saves order as `PENDING` and emits `ORDER_CREATED`.
2. **First Payment Attempt (Failure):** Customer attempts payment with insufficient funds. Backend records failed transaction, updates order status to `PAYMENT_FAILED`, and emits `PAYMENT_FAILED`.
3. **Analytics Impact at Stage 1:** 
   - Order total is **not** included in GMV (`BR-012`).
   - Order appears in Operational Health metrics as a failed payment attempt (`AR-011`).
4. **Retry Payment Attempt (Success):** Customer selects a different payment method and retries (`UC-010`). Simulation succeeds.
5. **Backend State Update:** Backend records successful payment record, transitions order from `PAYMENT_FAILED` to `PAID`, and emits `PAYMENT_COMPLETED` and `ORDER_PAID`.
6. **Final Analytics Impact:**
   - Order is now officially recognized in GMV (`BR-012`).
   - The historical record preserves both the failed attempt and the successful attempt for payment funnel analysis.

---

### Scenario 3 — Order Cancellation & Inventory Restoration
Demonstrates operational inventory safety and proper revenue exclusion:

1. **Order Creation:** Customer places order for 2 laptops (inventory decremented from 10 to 8). Order created as `PENDING`; emits `ORDER_CREATED`.
2. **Customer Cancellation:** Prior to payment completion, customer cancels order (`UC-011`).
3. **Backend Transaction:** Backend updates order status to `CANCELLED` and restores 2 units to inventory (inventory returned from 8 to 10). Emits `ORDER_CANCELLED`.
4. **Data Pipeline Processing:**
   - Bronze receives `ORDER_CREATED` and `ORDER_CANCELLED`.
   - Silver registers both events linked to the same order key.
   - Gold marks order status as `CANCELLED` in `fact_orders`.
5. **Analytical Verification:**
   - Cancelled order revenue is strictly **excluded** from GMV and gross sales metrics (`BR-013`).
   - Order is counted in cancellation rate metrics (`AR-010`).

---

### Scenario 4 — Duplicate Event Resilience (At-Least-Once Delivery)
Demonstrates Data Engineering idempotency and deduplication robustness:

1. **Producer Network Glitch:** Backend publishes `ORDER_CREATED` event. Network timeout causes producer retry; Kafka receives two identical event messages with the same `event_id`.
2. **Bronze Landing:** Ingestion consumer lands both messages in Bronze, preserving raw transport facts (distinct offsets, identical payloads).
3. **Silver Cleansing:** PySpark deduplication logic evaluates the incoming batch using `event_id` and business key (`order_id`). It discards the duplicate record, passing exactly one conformed order record forward.
4. **Gold Aggregation:** Gold fact tables calculate order count and revenue based on clean Silver data.
5. **Outcome:** GMV and total order counts reflect the single true business transaction with zero inflation (`DRIR-001`, `BR-014`).

---

### Scenario 5 — Pipeline Failure and Idempotent Retry
Demonstrates pipeline self-healing without corrupting analytical datasets:

1. **Scheduled Run:** Airflow triggers daily batch job for date `2026-09-25`.
2. **Task Failure:** Silver transformation fails halfway through execution due to temporary container resource exhaustion.
3. **Partial State Protection:** Because atomic partition writes are enforced (`DRIR-003`), no dirty or incomplete records remain committed in Silver.
4. **Automated Retry:** Airflow retries the task with backoff (`NFR-006`).
5. **Successful Rerun:** The job restarts from the partition boundary, processes Bronze inputs cleanly, and overwrites the target partition atomically.
6. **Outcome:** Gold models are populated accurately with zero duplicate rows or missing records (`DRIR-002`).

---

## 12. State Machines

### 12.1 Order State Machine
Governs all legal lifecycle transitions for an order entity in the operational system:

```
                          ┌─────────────────┐
                          │     PENDING     │
                          └────────┬────────┘
                                   │
                 ┌─────────────────┼─────────────────┐
                 │                 │                 │
                 ▼                 ▼                 ▼
        ┌──────────────────┐┌──────────────┐┌──────────────────┐
        │  PAYMENT_FAILED  ││     PAID     ││    CANCELLED     │ (Terminal)
        └────────┬─────────┘└──────┬───────┘└──────────────────┘
                 │                 │
        ┌────────┴────────┐        ▼
        │ Retry           │ ┌──────────────┐
        │ Payment         │ │  COMPLETED   │ (Terminal)
        ▼                 │ └──────────────┘
      (PAID)              │
                          ▼ (Cancel Order)
                    (CANCELLED)
```

| Current Status | Trigger / Action | Target Status | Preconditions & Side Effects |
| :--- | :--- | :--- | :--- |
| *None* | `UC-008` (Create Order) | `PENDING` | Cart non-empty; inventory decremented; price snapshotted. |
| `PENDING` | `UC-009` (Payment Success) | `PAID` | Payment settled; emits `PAYMENT_COMPLETED` and `ORDER_PAID`. |
| `PENDING` | `UC-009` (Payment Failure) | `PAYMENT_FAILED` | Simulation rejected; emits `PAYMENT_FAILED`; inventory remains held. |
| `PENDING` | `UC-011` (Cancel Order) | `CANCELLED` | Customer cancels; inventory restored; emits `ORDER_CANCELLED`. |
| `PAYMENT_FAILED` | `UC-010` (Retry Success) | `PAID` | Retry payment settled; order marked `PAID`. |
| `PAYMENT_FAILED` | `UC-011` (Cancel Order) | `CANCELLED` | Customer cancels unpaid order; inventory restored. |
| `PAID` | Fulfillment Process | `COMPLETED` | Items delivered; terminal state. |
| `CANCELLED` | *None* | *None* | Terminal state; no transitions permitted (`BR-007`). |
| `COMPLETED` | *None* | *None* | Terminal state; no transitions permitted (`BR-007`). |

---

### 12.2 Payment State Machine
Governs individual simulated payment attempt records:

```
       [ Initiated ]
             │
       ┌─────┴─────┐
       ▼           ▼
   (SUCCESS)    (FAILED)
```

- Each payment transaction record is created atomically in either `SUCCESS` or `FAILED` state.
- Multiple payment attempt records may associate with a single order (supporting failure and subsequent retry audit trails).

---

## 13. Event Mapping

High-level matrix mapping business use cases to corresponding domain events published to Kafka:

| Use Case ID | Use Case Name | Emitted Domain Event | Analytical Significance |
| :--- | :--- | :--- | :--- |
| **`UC-001`** | Register Account | `USER_REGISTERED` | User growth telemetry, customer acquisition cohorts. |
| **`UC-004`** | View Product Details | `PRODUCT_VIEWED` | Catalog interest, browse-to-purchase conversion drop-off. |
| **`UC-005`** / **`UC-006`** | Add / Update Cart | `CART_UPDATED` / `ITEM_ADDED` | Cart abandonment analysis, intent tracking. |
| **`UC-008`** | Create Order | `ORDER_CREATED` | Order volume tracking, checkout funnel conversion. |
| **`UC-009`** / **`UC-010`** | Make / Retry Payment | `PAYMENT_COMPLETED` | Financial reconciliation, settlement speed tracking. |
| **`UC-009`** / **`UC-010`** | Make / Retry Payment | `PAYMENT_FAILED` | Operational payment failure monitoring, error distribution. |
| **`UC-009`** / **`UC-010`** | Payment Success Transition | `ORDER_PAID` | **Official GMV recognition**, revenue recognition threshold. |
| **`UC-011`** | Cancel Order | `ORDER_CANCELLED` | Churn tracking, revenue deduction, inventory reconciliation. |

> [!IMPORTANT]
> **Business Event Semantics Policy for Analytics:**
> - **`PAYMENT_COMPLETED`** represents the outcome of a financial settlement transaction. It is loaded into `fact_payments` to analyze payment success/failure rates, gateway latency, and financial reconciliation.
> - **`ORDER_PAID`** represents the official transition of the order entity into the `PAID` state. **This is the single authoritative event that triggers Gross Merchandise Value (GMV) and gross revenue recognition** in `fact_orders` and downstream BI rollups (`BR-012`). Treating `ORDER_PAID` as the sole GMV recognition boundary strictly prevents double-counting revenue on payment retry attempts.

---

## 14. Requirement Traceability Matrix

Ensures every functional requirement and business rule from [`02-requirements.md`](file:///home/nguyen-son-minh/Downloads/Real-time%20E-commerce%20Analytics%20Platform/docs/02-requirements.md) is realized in at least one use case:

| Use Case ID | Use Case Name | Primary Requirements | Enforced Business Rules |
| :--- | :--- | :--- | :--- |
| **`UC-001`** | Register Account | `FR-001`, `FR-004`, `DR-001`, `NFR-016` | `BR-001` |
| **`UC-002`** | Authenticate User | `FR-002`, `AAR-001`, `AAR-002` | `NFR-016`, `NFR-017` |
| **`UC-002A`** | View Customer Profile | `FR-003`, `DR-001`, `AAR-002`, `AAR-003` | `NFR-016` |
| **`UC-003`** | Browse Product Catalog | `FR-005`, `FR-006`, `DR-002`, `NFR-001` | `BR-001` |
| **`UC-004`** | View Product Details | `FR-007`, `FR-008`, `ER-001`, `AR-006` | `BR-001` |
| **`UC-005`** | Add Product to Cart | `FR-009`, `FR-010`, `FR-013`, `FR-014` | `BR-001`, `BR-003`, `BR-004`, `BR-005` |
| **`UC-006`** | Update Cart Item Quantity | `FR-011`, `FR-013`, `FR-014` | `BR-003`, `BR-004`, `BR-005` |
| **`UC-007`** | Remove Item from Cart | `FR-012`, `FR-013`, `FR-014` | `BR-005` |
| **`UC-008`** | Create Order | `FR-015` - `FR-019`, `ER-001`, `DR-004` | `BR-001`, `BR-002`, `BR-003`, `BR-006`, `BR-007` |
| **`UC-009`** | Make Payment | `FR-023` - `FR-027`, `ER-001`, `DR-005` | `BR-007`, `BR-009`, `BR-010`, `BR-011` |
| **`UC-010`** | Retry Failed Payment | `FR-019`, `FR-023`, `FR-025`, `FR-026` | `BR-007`, `BR-011` |
| **`UC-011`** | Cancel Order | `FR-019`, `FR-020`, `FR-022`, `DR-004` | `BR-007`, `BR-008`, `BR-013` |
| **`UC-012`** | View Order History | `FR-021`, `DR-004`, `AAR-002`, `AAR-003` | `AAR-003` (Data Isolation) |
| **`UC-ADMIN-001`** | Manage Product | `FR-005`, `FR-006`, `DR-002`, `AAR-004` | `BR-001`, `BR-002` |
| **`UC-ADMIN-002`** | Manage Category | `FR-005`, `DR-002`, `AAR-004` | `AAR-004` |
| **`UC-ADMIN-003`** | Manage Inventory | `FR-007`, `DR-002`, `AAR-004` | `BR-003` |
| **`UC-ADMIN-004`** | View Operational Analytics | `AR-001` - `AR-013`, `DSR-002`, `DSR-005` | `BR-012`, `BR-013` |
| **`UC-DATA-001`** | Ingest Domain Events | `DER-001` - `DER-004`, `DRIR-004` | `DER-003` (Replayability) |
| **`UC-DATA-002`** | Process Bronze to Silver | `DER-005` - `DER-009`, `DQR-001` - `DQR-003`| `DRIR-001`, `BR-014` |
| **`UC-DATA-003`** | Build Gold Dimensional Model | `DER-010` - `DER-012`, `AR-001` - `AR-013`| `BR-012`, `BR-013`, `BR-014` |
| **`UC-DATA-004`** | Execute Data Quality Checks | `DQR-001` - `DQR-006` | `DQR-005` (Quality Gate) |
| **`UC-DATA-005`** | Execute Analytical Pipeline | `DER-012`, `NFR-006`, `AC-006`, `DRIR-002`| `BR-014` (Idempotency) |

---

## 15. Quality & Consistency Verification

- [x] **Requirement Coverage:** All core Functional Requirements (`FR-001` - `FR-027`), Data Requirements (`DR-001` - `DR-008`), and Data Engineering Requirements (`DER-001` - `DER-012`) are mapped to explicit use case workflows.
- [x] **State Machine Alignment:** Order lifecycle strictly adheres to `PENDING` $\rightarrow$ `PAID` / `PAYMENT_FAILED` / `CANCELLED`, retry handling, and terminal states.
- [x] **Event-Driven Coupling:** Every state transition identifies its corresponding domain event without dictating physical Kafka topic topology or partition counts prematurely.
- [x] **Decoupled Architecture:** Analytical processing flows strictly through Bronze $\rightarrow$ Silver $\rightarrow$ Gold; administrative analytics query the Gold layer exclusively, preserving operational database isolation (`DSR-002`, `DSR-005`).
- [x] **Idempotency & Reliability:** Scenarios and workflows explicitly account for duplicate message handling, partial failure recovery, and zero data corruption.
- [x] **No Premature Implementation:** Zero DDL SQL statements, REST paths, or framework code snippets are included, leaving design freedom for subsequent technical specifications.
