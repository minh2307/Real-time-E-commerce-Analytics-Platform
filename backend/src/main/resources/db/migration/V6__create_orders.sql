CREATE TABLE orders (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id     UUID NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    status          VARCHAR(50) NOT NULL DEFAULT 'PENDING',
    subtotal        NUMERIC(12, 2) NOT NULL,
    tax_amount      NUMERIC(12, 2) NOT NULL DEFAULT 0.00,
    shipping_amount NUMERIC(12, 2) NOT NULL DEFAULT 0.00,
    total_amount    NUMERIC(12, 2) NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_orders_status CHECK (status IN ('PENDING', 'PAID', 'PAYMENT_FAILED', 'CANCELLED', 'COMPLETED')),
    CONSTRAINT chk_orders_subtotal CHECK (subtotal >= 0.00),
    CONSTRAINT chk_orders_tax CHECK (tax_amount >= 0.00),
    CONSTRAINT chk_orders_shipping CHECK (shipping_amount >= 0.00),
    CONSTRAINT chk_orders_total CHECK (total_amount = subtotal + tax_amount + shipping_amount)
);

CREATE INDEX idx_orders_customer_created ON orders(customer_id, created_at DESC);
CREATE INDEX idx_orders_status ON orders(status);

CREATE TABLE order_items (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id      UUID NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    product_id    UUID NOT NULL REFERENCES products(id) ON DELETE RESTRICT,
    product_sku   VARCHAR(100) NOT NULL,
    product_title VARCHAR(255) NOT NULL,
    unit_price    NUMERIC(12, 2) NOT NULL,
    quantity      INTEGER NOT NULL,
    line_total    NUMERIC(12, 2) NOT NULL,
    CONSTRAINT chk_order_items_qty CHECK (quantity > 0),
    CONSTRAINT chk_order_items_price CHECK (unit_price >= 0.00),
    CONSTRAINT chk_order_items_total CHECK (line_total = unit_price * quantity)
);

CREATE INDEX idx_order_items_order_id ON order_items(order_id);
