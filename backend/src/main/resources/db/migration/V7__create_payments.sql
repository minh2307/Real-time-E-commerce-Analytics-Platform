CREATE TABLE payments (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id       UUID NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    attempt_number INTEGER NOT NULL DEFAULT 1,
    status         VARCHAR(50) NOT NULL,
    amount         NUMERIC(12, 2) NOT NULL,
    currency       VARCHAR(10) NOT NULL DEFAULT 'USD',
    payment_method VARCHAR(50) NOT NULL DEFAULT 'CREDIT_CARD',
    error_code     VARCHAR(50),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_payments_attempt CHECK (attempt_number >= 1),
    CONSTRAINT chk_payments_status CHECK (status IN ('SUCCESS', 'FAILED')),
    CONSTRAINT chk_payments_amount CHECK (amount >= 0.00),
    CONSTRAINT uq_order_attempt UNIQUE (order_id, attempt_number)
);

CREATE INDEX idx_payments_order_id ON payments(order_id);
