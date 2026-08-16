CREATE TABLE payments (
    order_id TEXT PRIMARY KEY,      -- which order this payment is for
    status   TEXT NOT NULL          -- 'charged' or 'failed'
);