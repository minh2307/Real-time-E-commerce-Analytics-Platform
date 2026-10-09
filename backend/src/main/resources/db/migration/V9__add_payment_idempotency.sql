ALTER TABLE payments ADD COLUMN idempotency_key VARCHAR(100);
ALTER TABLE payments ADD CONSTRAINT uq_payment_idempotency UNIQUE (idempotency_key);
