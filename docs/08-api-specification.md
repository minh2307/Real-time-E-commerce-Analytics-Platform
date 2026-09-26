# 08 — API Specification

## 1. Document Metadata
- **Project:** Real-time E-commerce Analytics Platform
- **Phase:** Phase 8 — API Specification
- **Purpose:** Define the HTTP REST API contract connecting the client layer to the Spring Boot monolithic backend.
- **Dependencies:** Documents `01-project-brief.md` through `07-data-pipeline.md`.

## 2. Purpose
This document specifies the RESTful HTTP API for the e-commerce backend. It serves as the authoritative contract for client-server interactions, covering authentication, catalog browsing, cart management, order placement, payments, and administrative functions. It explicitly outlines how API requests interact with the PostgreSQL transactional database and map to Outbox events.

## 3. Scope
**In Scope:**
- Customer-facing APIs (Auth, Profile, Catalog, Cart, Orders, Payments, Product Views).
- Admin-facing APIs (Catalog management, Inventory management).
- Transactional and concurrency behaviors of endpoints.
- Error models, idempotency, and pagination.

**Out of Scope:**
- Analytical APIs or direct BI querying against PostgreSQL (Analytics isolation rule applies).
- Implementation details of Spring controllers, DTOs, or database repositories.
- Specific JWT library details or exact token claims (TBD during implementation).

## 4. API Design Principles
- **Resource-Oriented REST:** Uses standard HTTP methods (GET, POST, PUT, DELETE) mapped to domain resources.
- **Transactional Consistency:** API endpoints that alter business state guarantee atomic updates to PostgreSQL and the `outbox_events` table within a single database transaction.
- **Decoupled Asynchronous Events:** APIs do not write directly to Kafka. Event propagation is handled asynchronously via the Transactional Outbox pattern.
- **Idempotency:** Critical state-mutating requests (e.g., order creation, payments) require idempotency keys to prevent duplicate processing.
- **Fail-Fast Validation:** APIs strictly validate input formats and business rules before attempting state mutations.

## 5. Base URL and Versioning
- **Base URL Pattern:** `/api/v1/`
- **Versioning Policy:** API versioning is embedded in the URI (`v1`). Breaking changes will introduce new endpoints or a `v2` namespace.

## 6. Authentication & Authorization
The API uses token-based authentication (e.g., Bearer tokens). The exact token format (e.g., JWT) is a TBD implementation decision.

- **Public Endpoints:** No token required. Includes product browsing, category listing, and user registration/login.
- **Customer Endpoints:** Requires a valid token. Enforces ownership (users can only access their own profiles, carts, and orders).
- **Admin Endpoints:** Requires a valid token with administrative privileges (e.g., an `ADMIN` role claim).

## 7. Common Headers
- `Authorization: Bearer <token>`: Required for protected endpoints.
- `Content-Type: application/json`: Used for request and response bodies.
- `Accept: application/json`: Preferred response format.
- `Idempotency-Key: <UUID>`: Required for `POST` operations on orders and payments to prevent accidental duplicates.

## 8. Common Response Format
Successful API responses return raw JSON objects or arrays reflecting the requested resource. For collections with pagination, a wrapped structure is returned.

**Single Resource:**
```json
{
  "id": "uuid-1234",
  "name": "Example"
}
```

**Paginated Collection (Example format; exact schema TBD):**
```json
{
  "data": [ ... ],
  "page": 0,
  "size": 20,
  "totalElements": 100,
  "totalPages": 5
}
```

## 9. Error Model
The API uses a standardized JSON error response structure. (Exact field set is TBD — implementation decision, but conceptually follows this schema).

```json
{
  "timestamp": "2026-09-26T10:00:00Z",
  "status": 400,
  "code": "VALIDATION_ERROR",
  "message": "Invalid input data",
  "path": "/api/v1/orders",
  "details": [
    {
      "field": "items[0].quantity",
      "issue": "Must be greater than 0"
    }
  ]
}
```

**Standard HTTP Status Codes:**
- `200 OK`: Successful read or update.
- `201 Created`: Successful creation of a new resource.
- `204 No Content`: Successful deletion or action with no response body.
- `400 Bad Request`: Validation failure or malformed syntax.
- `401 Unauthorized`: Missing or invalid authentication token.
- `403 Forbidden`: Authenticated, but lacks required permissions (e.g., accessing another user's order).
- `404 Not Found`: Resource does not exist.
- `409 Conflict`: Business rule violation or concurrency conflict (e.g., insufficient inventory, invalid state transition).
- `422 Unprocessable Entity`: Semantically invalid request (optional, may overlap with 400/409 based on implementation decision).
- `500 Internal Server Error`: Unexpected system failure.

## 10. Pagination and Sorting
Endpoints returning collections support basic pagination via query parameters. (Cursor pagination is TBD/Not required unless explicitly needed).
- `page`: Page index (default: 0).
- `size`: Items per page (default: 20).
- `sort`: Sorting criteria (e.g., `sort=createdAt,desc`).

## 11. Idempotency
Critical state-mutating requests must include an `Idempotency-Key` header.
- **Behavior:** The API backend will check if a request with the given key was already processed successfully. If so, it returns the cached successful response without re-executing the transaction. A retried request must not accidentally create duplicate business state or duplicate financial recognition.
- **Scope:** Applies to Order Creation, Payment Attempts, Payment Retries, and Order Cancellation.
- **Implementation:** Exact persistence/TTL semantics for idempotency keys are TBD — implementation decision.

## 12. API Resource Overview
- `/api/v1/auth`: Registration, login.
- `/api/v1/users`: Customer profiles.
- `/api/v1/categories`: Catalog categories.
- `/api/v1/products`: Product catalog and views.
- `/api/v1/cart`: Active cart management.
- `/api/v1/orders`: Order lifecycle operations.
- `/api/v1/payments`: Payment capture and processing.
- `/api/v1/admin/inventory`: Inventory management.

---

## 13. Authentication APIs

### 13.1 Register
- **Method & Path:** `POST /api/v1/auth/register`
- **Authentication:** None (Public)
- **Purpose:** Create a new user profile.
- **Request Body:** Email, password (plain text, to be hashed securely), first name, last name.
- **Success Response:** `201 Created` with created user details (excluding password).
- **Error Responses:** `400 Bad Request` (validation), `409 Conflict` (email already exists).
- **Event/Outbox:** Controlled Reference Ingestion boundary (no direct transactional event).

### 13.2 Login
- **Method & Path:** `POST /api/v1/auth/login`
- **Authentication:** None (Public)
- **Purpose:** Authenticate and retrieve a token.
- **Request Body:** Email, password.
- **Success Response:** `200 OK` with token (e.g., `{ "accessToken": "..." }`).
- **Error Responses:** `401 Unauthorized` (invalid credentials).

---

## 14. User APIs

### 14.1 Get Current Profile
- **Method & Path:** `GET /api/v1/users/me`
- **Authentication:** Required
- **Purpose:** Retrieve the authenticated customer's profile.
- **Success Response:** `200 OK` with user details (excluding password hash).
- **Error Responses:** `401 Unauthorized`.

---

## 15. Category APIs

### 15.1 List Categories
- **Method & Path:** `GET /api/v1/categories`
- **Authentication:** None (Public)
- **Purpose:** Retrieve active categories. Supports pagination.
- **Success Response:** `200 OK` (Paginated list of categories).

### 15.2 Get Category
- **Method & Path:** `GET /api/v1/categories/{categoryId}`
- **Authentication:** None (Public)
- **Purpose:** Retrieve details for a specific category.
- **Success Response:** `200 OK`.
- **Error Responses:** `404 Not Found`.

### 15.3 Admin Category Operations
- **Method & Path:** `POST / PUT / DELETE /api/v1/admin/categories`
- **Authentication:** Required (Admin only)
- **Purpose:** Manage catalog categories.
- **Event/Outbox:** Controlled reference export updates `dim_categories` (SCD Type 1). No explicit outbox event.

---

## 16. Product APIs

### 16.1 List Products
- **Method & Path:** `GET /api/v1/products`
- **Authentication:** None (Public)
- **Purpose:** Browse active products.
- **Query Params:** `categoryId`, `page`, `size`, `sort`.
- **Success Response:** `200 OK` (Paginated list).

### 16.2 Get Product
- **Method & Path:** `GET /api/v1/products/{productId}`
- **Authentication:** None (Public)
- **Purpose:** Retrieve full product details, including current price.
- **Success Response:** `200 OK`.
- **Error Responses:** `404 Not Found`.

### 16.3 Product View
- **Method & Path:** `POST /api/v1/products/{productId}/views`
- **Authentication:** Optional (Supports guest Sentinel ID `-1` if unauthenticated).
- **Purpose:** Record a user clicking/viewing a product page for analytics.
- **Success Response:** `202 Accepted` or `204 No Content`.
- **Event/Outbox:** Emits `PRODUCT_VIEWED` outbox event atomically.

### 16.4 Admin Product Operations
- **Method & Path:** `POST / PUT / DELETE /api/v1/admin/products`
- **Authentication:** Required (Admin only)
- **Purpose:** Manage product catalog and pricing.
- **Event/Outbox:** Controlled reference export updates `dim_products` (SCD Type 1). No explicit outbox event.

---

## 17. Cart APIs

### 17.1 Get Cart
- **Method & Path:** `GET /api/v1/cart`
- **Authentication:** Required (Customer)
- **Purpose:** Retrieve the current active cart and its items for the authenticated user.
- **Success Response:** `200 OK`.

### 17.2 Add Cart Item
- **Method & Path:** `POST /api/v1/cart/items`
- **Authentication:** Required (Customer)
- **Purpose:** Add a product to the cart.
- **Request Body:** `{ "productId": "...", "quantity": 1 }`
- **Success Response:** `200 OK` (Updated cart).
- **Error Responses:** `400 Bad Request` (invalid quantity), `404 Not Found` (product doesn't exist).
- **Event/Outbox:** Emits `ITEM_ADDED_TO_CART` outbox event.

### 17.3 Update Cart Item
- **Method & Path:** `PUT /api/v1/cart/items/{productId}`
- **Authentication:** Required (Customer)
- **Purpose:** Update the quantity of a cart item.
- **Event/Outbox:** No specific event defined in MVP (TBD or relies on added/removed).

### 17.4 Remove Cart Item
- **Method & Path:** `DELETE /api/v1/cart/items/{productId}`
- **Authentication:** Required (Customer)
- **Purpose:** Remove an item from the cart.
- **Success Response:** `200 OK` or `204 No Content`.
- **Event/Outbox:** Emits `ITEM_REMOVED_FROM_CART` outbox event.

### 17.5 Clear Cart
- **Method & Path:** `DELETE /api/v1/cart`
- **Authentication:** Required (Customer)
- **Purpose:** Empty the cart entirely.

---

## 18. Order APIs

### 18.1 Create Order
- **Method & Path:** `POST /api/v1/orders`
- **Authentication:** Required (Customer)
- **Headers:** `Idempotency-Key` (Required)
- **Purpose:** Convert cart/items into a confirmed order.
- **Request Body:** Selected items, shipping/billing details.
- **Success Response:** `201 Created` with order details (Status: `PENDING`).
- **Error Responses:** `409 Conflict` (Insufficient inventory, price mismatch).
- **Transactional Behavior:** Atomically inserts into `orders`, `order_items` (capturing `unit_price`, `product_sku`, `product_title` snapshots), decrements `inventory`, and inserts `outbox_events`.
- **Event/Outbox:** Emits `ORDER_CREATED` outbox event.

### 18.2 Get Order
- **Method & Path:** `GET /api/v1/orders/{orderId}`
- **Authentication:** Required (Customer ownership enforced)
- **Purpose:** Retrieve full details of an order, including line items.
- **Success Response:** `200 OK`.
- **Error Responses:** `403 Forbidden` (If requested by a different user).

### 18.3 Order History
- **Method & Path:** `GET /api/v1/orders`
- **Authentication:** Required (Customer)
- **Purpose:** List historical orders for the authenticated user.
- **Query Params:** `page`, `size`, `sort`.
- **Success Response:** `200 OK` (Paginated list).

### 18.4 Cancel Order
- **Method & Path:** `POST /api/v1/orders/{orderId}/cancel`
- **Authentication:** Required (Customer ownership enforced)
- **Headers:** `Idempotency-Key` (Recommended)
- **Purpose:** Cancel an order that is in a non-terminal, cancellable state (e.g., `PENDING` or `PAYMENT_FAILED`).
- **Success Response:** `200 OK` (Order status updated to `CANCELLED`).
- **Error Responses:** `409 Conflict` (Cannot cancel an order that is `PAID` or `COMPLETED`).
- **Transactional Behavior:** Atomically updates `orders` status to `CANCELLED`, restores `inventory`, and inserts `outbox_events`.
- **Event/Outbox:** Emits `ORDER_CANCELLED` outbox event.

---

## 19. Payment APIs

### 19.1 Create Payment Attempt
- **Method & Path:** `POST /api/v1/orders/{orderId}/payments`
- **Authentication:** Required (Customer ownership enforced)
- **Headers:** `Idempotency-Key` (Required)
- **Purpose:** Process a payment for a `PENDING` order.
- **Request Body:** Payment details (e.g., card token).
- **Success Response:** `200 OK` (Payment successful, Order status transitions to `PAID`).
- **Error Responses:** `400 Bad Request` (Payment failed validation), `409 Conflict` (Payment declined by gateway, Order transitions to `PAYMENT_FAILED`).
- **Transactional Behavior:** Atomically inserts into `payments` table and updates `orders` state based on gateway result.
- **Event/Outbox:** 
  - If successful: Emits `PAYMENT_COMPLETED` (payment audit) AND `ORDER_PAID` (order transition).
  - If failed: Emits `PAYMENT_FAILED`.

### 19.2 Payment Retry
- **Method & Path:** `POST /api/v1/orders/{orderId}/payments/retry`
- **Authentication:** Required (Customer ownership enforced)
- **Headers:** `Idempotency-Key` (Required)
- **Purpose:** Retry payment for an order in `PAYMENT_FAILED` state.
- **Behavior:** Operates identically to `Create Payment Attempt`. Multiple retries generate multiple rows in `payments` (and multiple `PAYMENT_FAILED`/`PAYMENT_COMPLETED` events), but upon success, only one `ORDER_PAID` transition occurs.

### 19.3 Payment Result
*(Depending on implementation, payment callbacks/webhooks from a 3rd party gateway may use a dedicated endpoint. TBD — implementation decision).*

---

## 20. Inventory APIs

### 20.1 Admin Inventory Operations
- **Method & Path:** `PUT /api/v1/admin/inventory/{productId}`
- **Authentication:** Required (Admin only)
- **Purpose:** Directly mutate inventory levels (e.g., restocking).
- **Note:** This is strictly separated from order creation reservation (which is handled internally within the `POST /orders` transaction).

---

## 21. Order State Machine
The API strictly enforces the operational order state machine:

| Current State | API Action | Target State | Outbox Event(s) Emitted | Allowed? |
|---|---|---|---|---|
| (None) | `POST /orders` | **PENDING** | `ORDER_CREATED` | Yes |
| **PENDING** | `POST /payments` (Success) | **PAID** | `PAYMENT_COMPLETED`, `ORDER_PAID` | Yes |
| **PENDING** | `POST /payments` (Failure) | **PAYMENT_FAILED** | `PAYMENT_FAILED` | Yes |
| **PENDING** | `POST /orders/.../cancel` | **CANCELLED** | `ORDER_CANCELLED` | Yes |
| **PAYMENT_FAILED**| `POST /payments/retry` (Success)| **PAID** | `PAYMENT_COMPLETED`, `ORDER_PAID` | Yes |
| **PAYMENT_FAILED**| `POST /orders/.../cancel` | **CANCELLED** | `ORDER_CANCELLED` | Yes |
| **PAID** | Fulfillment (Internal process) | **COMPLETED** | (None - Internal state only) | System Internal |
| **PAID** | `POST /orders/.../cancel` | (Blocked) | - | No (Returns 409) |
| **CANCELLED** | (Any action) | (Terminal) | - | No (Returns 409) |
| **COMPLETED** | (Any action) | (Terminal) | - | No (Returns 409) |

**Important Rules on `COMPLETED`:**
- `COMPLETED` is an operational OLTP lifecycle state representing fulfillment.
- `COMPLETED` is **NOT** an analytics event. There is no `ORDER_COMPLETED` outbox event.
- `ORDER_PAID` remains the sole, authoritative trigger for GMV recognition.

---

## 22. API → Transaction → Outbox → Kafka Mapping

| API Endpoint | PostgreSQL Transaction Effect | Generated Outbox Event | Kafka Topic Destination |
|---|---|---|---|
| `POST /products/{id}/views` | Insert view log (or skip OLTP table) | `PRODUCT_VIEWED` | `ecommerce.views` |
| `POST /cart/items` | Update cart table | `ITEM_ADDED_TO_CART` | `ecommerce.carts` |
| `DELETE /cart/items/{id}`| Update cart table | `ITEM_REMOVED_FROM_CART`| `ecommerce.carts` |
| `POST /orders` | Insert `orders`, `order_items`, decrement `inventory` | `ORDER_CREATED` | `ecommerce.orders` |
| `POST /orders/{id}/cancel`| Update `orders.status = CANCELLED`, restore `inventory` | `ORDER_CANCELLED` | `ecommerce.orders` |
| `POST /payments` (Success)| Insert `payments` row, Update `orders.status = PAID` | `PAYMENT_COMPLETED` (Audit), `ORDER_PAID` (Order Transition) | `ecommerce.payments`, `ecommerce.orders` |
| `POST /payments` (Fail) | Insert `payments` row, Update `orders.status = PAYMENT_FAILED` | `PAYMENT_FAILED` | `ecommerce.payments` |

*Note on Missing Events:* Any action not listed here (e.g., `PUT /cart/items` quantity update) either emits no specific event or relies on an implementation decision yet to be finalized (TBD). The MVP processes only defined upstream events.

---

## 23. API → Data Pipeline Lineage

The API operates exclusively in the operational zone, maintaining complete isolation from the analytical zone.

```text
       [OPERATIONAL ZONE]                             [ANALYTICAL ZONE]

Client HTTP Request
       │
Spring Boot API Controller
       │
Application Service
       │
PostgreSQL Transaction (OLTP)
       │
Transactional Outbox (outbox_events)
       │
       ▼
Kafka Message Broker (Decoupled Boundary) ────────► Ingestion Daemon
                                                           │
                                                           ▼
                                                       Bronze Lake
                                                           │
                                                           ▼
                                                      Silver Cleansed
                                                           │
                                                           ▼
                                                       Gold Facts
```

- **Analytics Isolation:** No API endpoint is provided for directly querying Gold facts or analytical models from PostgreSQL. Analytical and BI querying is intentionally excluded from this backend HTTP contract.

---

## 24. Validation Rules
- **Order Creation:** Must validate that the cart is not empty, products exist, and sufficient inventory is available. `unit_price` in the generated `order_items` must accurately reflect the snapshot price at the exact moment of order creation.
- **Payment Retry:** Only permitted if the order is in `PAYMENT_FAILED` state.
- **Cancellation:** Only permitted for `PENDING` and `PAYMENT_FAILED` orders.

---

## 25. Concurrency and Conflict Handling
- **Inventory Updates (Order Creation):** Order creation must use safe database concurrency controls (e.g., `UPDATE inventory SET qty = qty - X WHERE qty >= X`) to prevent overselling. If inventory is insufficient, the transaction rolls back, returns `409 Conflict`, and no outbox events are published.
- **State Machine Transitions:** Using optimistic locking (e.g., `version` column) or specific WHERE clauses (`UPDATE orders SET status = 'PAID' WHERE id = ? AND status IN ('PENDING', 'PAYMENT_FAILED')`) prevents race conditions during concurrent payment webhooks/retries.

---

## 26. Security Considerations
- **Ownership Validation:** Endpoints like `/api/v1/orders/{orderId}` must verify that the `customerId` on the order matches the authenticated token's `userId`. Returning a `403 Forbidden` or generic `404 Not Found` (to prevent ID enumeration) is required.
- **Admin Boundaries:** Administrative endpoints (`/api/v1/admin/*`) strictly enforce role-based access control.
- **Data Minimization:** API responses omit sensitive fields like `password_hash`. They do not leak internal database surrogate keys unless they are designated as public identifiers (e.g., UUIDs).

---

## 27. API Traceability to Use Cases

| Documented Use Case (03-use-cases) | Supporting API Endpoint |
|---|---|
| User registers an account | `POST /api/v1/auth/register` |
| User browses catalog & views product | `GET /api/v1/products`, `POST /api/v1/products/{id}/views` |
| User adds item to cart | `POST /api/v1/cart/items` |
| User creates an order | `POST /api/v1/orders` |
| Order fails payment, user retries | `POST /api/v1/orders/{id}/payments/retry` |
| User cancels pending order | `POST /api/v1/orders/{id}/cancel` |

---

## 28. API Traceability to Requirements

| Requirement (02-requirements) | API / Contract Support |
|---|---|
| Functional: Catalog browsing | `/api/v1/products`, `/api/v1/categories` |
| Functional: Order placement | `/api/v1/orders` (Transactional with outbox) |
| Functional: Payment retry | `/api/v1/orders/{id}/payments/retry` (Multiple facts, one GMV trigger) |
| Technical: Modular Monolith | Single base URL (`/api/v1/`) with separated module paths |
| Architecture: Strict Isolation | No analytics queries in the API contract |

---

## 29. Implementation Boundaries / TBD
The following implementation-level decisions are intentionally deferred to the engineering phase and are explicitly **TBD**:
1. Exact JWT library, claims structure, and token expiration policy.
2. The specific persistence mechanism for `Idempotency-Key` (e.g., Redis vs. PostgreSQL table).
3. The exact JSON schema for the standard Error response model.
4. Specific configuration for cursor-based pagination (if standard offset pagination proves insufficient).
5. Webhook receiver endpoints for third-party payment gateways.

---

## 30. Consistency Checklist
- [x] Only docs/08-api-specification.md was created/modified
- [x] Documents 01–07 were read
- [x] API scope matches the use cases
- [x] API scope matches requirements
- [x] API scope matches database design
- [x] API scope matches architecture
- [x] API scope matches data pipeline
- [x] PostgreSQL remains transactional source of truth
- [x] Transactional Outbox is preserved
- [x] Kafka remains decoupled event path
- [x] No direct analytical PostgreSQL path is introduced
- [x] No invented event exists
- [x] No ORDER_COMPLETED event exists
- [x] COMPLETED remains an OLTP lifecycle state
- [x] ORDER_PAID remains sole GMV recognition trigger
- [x] PAYMENT_COMPLETED is not treated as GMV trigger
- [x] Payment retries do not create duplicate GMV
- [x] Order-item snapshots are preserved
- [x] Order state machine is consistent
- [x] Inventory concurrency behavior is consistent
- [x] Customer ownership/security is documented
- [x] Admin authorization is documented
- [x] Error model is consistent
- [x] Idempotency is addressed
- [x] Pagination is addressed
- [x] API versioning is addressed
- [x] API → Outbox/Kafka mapping is explicit
- [x] API → Data Pipeline lineage is explicit
- [x] Traceability exists
- [x] Unsupported details are marked TBD
- [x] No unnecessary technology was introduced
- [x] No Java/Spring implementation was created

---

## 31. Design Status

READY FOR IMPLEMENTATION

The API specification is consistent with documents 01–07,
preserves the modular-monolith and event-driven architecture,
uses only defined business concepts and event semantics,
and provides the HTTP contract required for backend implementation.

Any unspecified implementation-level details are explicitly marked TBD.
