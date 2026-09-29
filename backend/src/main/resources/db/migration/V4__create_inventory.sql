CREATE TABLE inventory (
    product_id         UUID PRIMARY KEY REFERENCES products(id) ON DELETE CASCADE,
    available_quantity INTEGER NOT NULL DEFAULT 0,
    reserved_quantity  INTEGER NOT NULL DEFAULT 0,
    version            BIGINT NOT NULL DEFAULT 0,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_inventory_available CHECK (available_quantity >= 0),
    CONSTRAINT chk_inventory_reserved CHECK (reserved_quantity >= 0)
);
