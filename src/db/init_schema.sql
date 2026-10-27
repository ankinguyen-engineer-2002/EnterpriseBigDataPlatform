-- =============================================================================
-- ENTERPRISE COMMERCE DATA PLATFORM - OLTP & LEGACY DW RECONCILIATION SCHEMA
-- 9 Relational OLTP Tables (124 Columns) + 1 Legacy Reconciliation Table (8 Columns) = 132 Columns
-- =============================================================================

-- 1. DISTRIBUTION CENTERS (8 columns - Supply Chain / Logistics network)
CREATE TABLE IF NOT EXISTS distribution_centers (
    dc_id VARCHAR(32) PRIMARY KEY,
    dc_code VARCHAR(16) NOT NULL UNIQUE,
    name VARCHAR(128) NOT NULL,
    region VARCHAR(32) NOT NULL,
    city VARCHAR(64) NOT NULL,
    country VARCHAR(64) NOT NULL DEFAULT 'Vietnam',
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL
);

-- 2. SELLERS (9 columns - Marketplace Merchants)
CREATE TABLE IF NOT EXISTS sellers (
    seller_id VARCHAR(32) PRIMARY KEY,
    seller_name VARCHAR(128) NOT NULL,
    tax_code VARCHAR(32) NOT NULL,
    city VARCHAR(64) NOT NULL,
    state VARCHAR(64) NOT NULL,
    seller_tier VARCHAR(32) NOT NULL DEFAULT 'Standard',
    rating_score NUMERIC(3, 2) NOT NULL DEFAULT 4.50,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    joined_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 3. CUSTOMERS (18 columns - SCD Type 2 & PII Governance target)
CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR(32) PRIMARY KEY,
    email VARCHAR(128) NOT NULL,
    phone VARCHAR(32) NOT NULL,
    first_name VARCHAR(64) NOT NULL,
    last_name VARCHAR(64) NOT NULL,
    gender VARCHAR(16) NOT NULL,
    birth_year INTEGER NOT NULL,
    city VARCHAR(64) NOT NULL,
    state VARCHAR(64) NOT NULL,
    country VARCHAR(64) NOT NULL DEFAULT 'Vietnam',
    postal_code VARCHAR(16) NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    traffic_source VARCHAR(32) NOT NULL,
    segment VARCHAR(32) NOT NULL DEFAULT 'Regular',
    device_type VARCHAR(32) NOT NULL,
    signup_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 4. PRODUCTS (18 columns - 75% 'electronics' Skew & SCD Type 2 target)
CREATE TABLE IF NOT EXISTS products (
    product_id VARCHAR(32) PRIMARY KEY,
    sku VARCHAR(64) NOT NULL UNIQUE,
    name VARCHAR(256) NOT NULL,
    category VARCHAR(64) NOT NULL,
    subcategory VARCHAR(64) NOT NULL,
    department VARCHAR(64) NOT NULL,
    garment_group VARCHAR(64) NOT NULL,
    brand VARCHAR(64) NOT NULL,
    seller_id VARCHAR(32) NOT NULL REFERENCES sellers(seller_id),
    default_dc_id VARCHAR(32) NOT NULL REFERENCES distribution_centers(dc_id),
    cost_price NUMERIC(14, 2) NOT NULL,
    base_price NUMERIC(14, 2) NOT NULL,
    retail_price NUMERIC(14, 2) NOT NULL,
    weight_grams INTEGER NOT NULL DEFAULT 500,
    detail_desc TEXT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 5. INVENTORY ITEMS (9 columns - Stock & COGS tracking)
CREATE TABLE IF NOT EXISTS inventory_items (
    inventory_item_id VARCHAR(32) PRIMARY KEY,
    product_id VARCHAR(32) NOT NULL REFERENCES products(product_id),
    dc_id VARCHAR(32) NOT NULL REFERENCES distribution_centers(dc_id),
    batch_lot_no VARCHAR(32) NOT NULL,
    unit_cost NUMERIC(14, 2) NOT NULL,
    stock_status VARCHAR(32) NOT NULL DEFAULT 'available',
    created_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sold_ts TIMESTAMPTZ,
    updated_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 6. ORDERS (20 columns - 80% 'Ho Chi Minh City' Skew & SLA timestamps)
CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(32) PRIMARY KEY,
    customer_id VARCHAR(32) NOT NULL REFERENCES customers(customer_id),
    status VARCHAR(32) NOT NULL,
    shipping_city VARCHAR(64) NOT NULL,
    shipping_state VARCHAR(64) NOT NULL,
    shipping_country VARCHAR(64) NOT NULL DEFAULT 'Vietnam',
    coupon_code VARCHAR(32),
    item_count INTEGER NOT NULL DEFAULT 1,
    subtotal_amount NUMERIC(14, 2) NOT NULL,
    discount_amount NUMERIC(14, 2) NOT NULL DEFAULT 0.00,
    shipping_fee NUMERIC(14, 2) NOT NULL DEFAULT 0.00,
    tax_amount NUMERIC(14, 2) NOT NULL DEFAULT 0.00,
    total_amount NUMERIC(14, 2) NOT NULL,
    currency VARCHAR(8) NOT NULL DEFAULT 'VND',
    order_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    approved_ts TIMESTAMPTZ,
    delivered_carrier_ts TIMESTAMPTZ,
    delivered_customer_ts TIMESTAMPTZ,
    estimated_delivery_ts TIMESTAMPTZ,
    updated_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 7. ORDER ITEMS (17 columns - ~2% Offline Duplicates target in Parquet)
CREATE TABLE IF NOT EXISTS order_items (
    order_item_id VARCHAR(32) PRIMARY KEY,
    order_id VARCHAR(32) NOT NULL REFERENCES orders(order_id),
    product_id VARCHAR(32) NOT NULL REFERENCES products(product_id),
    seller_id VARCHAR(32) NOT NULL REFERENCES sellers(seller_id),
    inventory_item_id VARCHAR(32) REFERENCES inventory_items(inventory_item_id),
    dc_id VARCHAR(32) NOT NULL REFERENCES distribution_centers(dc_id),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(14, 2) NOT NULL,
    cost_price NUMERIC(14, 2) NOT NULL,
    discount_amount NUMERIC(14, 2) NOT NULL DEFAULT 0.00,
    tax_amount NUMERIC(14, 2) NOT NULL DEFAULT 0.00,
    shipping_share_amount NUMERIC(14, 2) NOT NULL DEFAULT 0.00,
    net_line_amount NUMERIC(14, 2) NOT NULL,
    item_status VARCHAR(32) NOT NULL DEFAULT 'fulfilled',
    created_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    shipped_ts TIMESTAMPTZ,
    delivered_ts TIMESTAMPTZ
);

-- 8. PAYMENTS (15 columns - Mid-dataset Schema Evolution 'failure_reason' & PII 'card_last4')
CREATE TABLE IF NOT EXISTS payments (
    payment_id VARCHAR(32) PRIMARY KEY,
    order_id VARCHAR(32) NOT NULL REFERENCES orders(order_id),
    customer_id VARCHAR(32) NOT NULL REFERENCES customers(customer_id),
    payment_sequential INTEGER NOT NULL DEFAULT 1,
    method VARCHAR(32) NOT NULL,
    provider VARCHAR(32) NOT NULL,
    installments INTEGER NOT NULL DEFAULT 1,
    amount NUMERIC(14, 2) NOT NULL,
    fee_amount NUMERIC(14, 2) NOT NULL DEFAULT 0.00,
    currency VARCHAR(8) NOT NULL DEFAULT 'VND',
    card_last4 VARCHAR(4),
    status VARCHAR(32) NOT NULL,
    payment_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    failure_reason VARCHAR(128)
);

-- 9. ORDER REVIEWS (10 columns - Post-purchase satisfaction & NLP feedback)
CREATE TABLE IF NOT EXISTS order_reviews (
    review_id VARCHAR(32) PRIMARY KEY,
    order_id VARCHAR(32) NOT NULL REFERENCES orders(order_id),
    customer_id VARCHAR(32) NOT NULL REFERENCES customers(customer_id),
    product_id VARCHAR(32) NOT NULL REFERENCES products(product_id),
    rating_score INTEGER NOT NULL CHECK (rating_score BETWEEN 1 AND 5),
    review_title VARCHAR(128) NOT NULL,
    review_comment TEXT NOT NULL,
    sentiment_label VARCHAR(16) NOT NULL,
    created_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    answered_ts TIMESTAMPTZ
);

-- 10. LEGACY DAILY REVENUE REPORT (8 columns - Stage 7 WAP Zero-Copy Reconciliation target)
CREATE TABLE IF NOT EXISTS legacy_daily_revenue_report (
    report_date DATE NOT NULL,
    shipping_city VARCHAR(64) NOT NULL,
    order_count BIGINT NOT NULL,
    distinct_customers BIGINT NOT NULL,
    gross_merchandise_value NUMERIC(18, 2) NOT NULL,
    total_discount NUMERIC(18, 2) NOT NULL,
    net_revenue NUMERIC(18, 2) NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (report_date, shipping_city)
);

-- Enable REPLICA IDENTITY FULL for Debezium CDC (captures full before/after row images in WAL)
ALTER TABLE distribution_centers REPLICA IDENTITY FULL;
ALTER TABLE sellers REPLICA IDENTITY FULL;
ALTER TABLE customers REPLICA IDENTITY FULL;
ALTER TABLE products REPLICA IDENTITY FULL;
ALTER TABLE inventory_items REPLICA IDENTITY FULL;
ALTER TABLE orders REPLICA IDENTITY FULL;
ALTER TABLE order_items REPLICA IDENTITY FULL;
ALTER TABLE payments REPLICA IDENTITY FULL;
ALTER TABLE order_reviews REPLICA IDENTITY FULL;

-- Indexes for high-throughput OLTP & analytical reconciliation queries
CREATE INDEX IF NOT EXISTS idx_orders_customer_ts ON orders(customer_id, order_ts);
CREATE INDEX IF NOT EXISTS idx_orders_shipping_city ON orders(shipping_city);
CREATE INDEX IF NOT EXISTS idx_products_category ON products(category);
CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_payments_order_id ON payments(order_id);
