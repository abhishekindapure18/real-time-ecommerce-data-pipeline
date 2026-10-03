CREATE TABLE IF NOT EXISTS ecommerce_metrics (
    metric_id SERIAL PRIMARY KEY,
    metric_name VARCHAR(100),
    metric_value DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS revenue_by_category (
    category VARCHAR(100),
    total_revenue DOUBLE PRECISION,
    purchase_count INTEGER
);

CREATE TABLE IF NOT EXISTS top_products (
    product_id INTEGER,
    product_name VARCHAR(100),
    units_sold INTEGER,
    revenue DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS realtime_metrics (
    metric_id SERIAL PRIMARY KEY,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    category VARCHAR(100),
    total_revenue DOUBLE PRECISION,
    total_units_sold INTEGER,
    total_purchases INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);