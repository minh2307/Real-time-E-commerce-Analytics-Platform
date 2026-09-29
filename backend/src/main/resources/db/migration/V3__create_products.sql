CREATE TABLE products (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    category_id UUID NOT NULL REFERENCES categories(id) ON DELETE RESTRICT,
    sku         VARCHAR(100) UNIQUE NOT NULL,
    title       VARCHAR(255) NOT NULL,
    description TEXT,
    price       NUMERIC(12, 2) NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_products_price CHECK (price >= 0.00)
);

CREATE INDEX idx_products_category_id ON products(category_id);
