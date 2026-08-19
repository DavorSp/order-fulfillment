CREATE TABLE orders (
    order_id   TEXT PRIMARY KEY,
    sku        TEXT NOT NULL,
    qty        INTEGER NOT NULL,
    status     TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
