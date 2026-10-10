CREATE SCHEMA IF NOT EXISTS staging;

CREATE SCHEMA IF NOT EXISTS warehouse;

CREATE SCHEMA IF NOT EXISTS analytics;


-- ============================================================
-- Staging
-- ============================================================

CREATE TABLE IF NOT EXISTS staging.sales_raw (
    customer_id VARCHAR(100),
    product_id VARCHAR(100),
    quantity NUMERIC(18, 2),
    price NUMERIC(18, 2),
    transaction_date TIMESTAMP,
    payment_method VARCHAR(100),
    store_location VARCHAR(100),
    product_category VARCHAR(100),
    discount_applied NUMERIC(10, 4),
    total_amount NUMERIC(18, 2)
);


-- ============================================================
-- Store dimension
-- ============================================================

CREATE TABLE IF NOT EXISTS warehouse.dim_store (
    store_id VARCHAR(100) PRIMARY KEY,
    store_name VARCHAR(150) NOT NULL,
    source_location VARCHAR(100) NOT NULL UNIQUE
);


-- ============================================================
-- Product dimension
--
-- Product category is not stored here because the source data
-- allows one product_id to occur under multiple categories.
-- ============================================================

CREATE TABLE IF NOT EXISTS warehouse.dim_product (
    product_id VARCHAR(100) PRIMARY KEY,
    product_name VARCHAR(150) NOT NULL,
    abc_quantity CHAR(1),
    abc_amount CHAR(1)
);


-- ============================================================
-- Sales fact
-- ============================================================

CREATE TABLE IF NOT EXISTS warehouse.fact_sales (
    sales_id BIGSERIAL PRIMARY KEY,
    customer_id VARCHAR(100),
    product_id VARCHAR(100) NOT NULL,
    store_id VARCHAR(100) NOT NULL,
    transaction_date TIMESTAMP NOT NULL,
    quantity NUMERIC(18, 2) NOT NULL CHECK (quantity > 0),
    price NUMERIC(18, 2) CHECK (price >= 0),
    product_category VARCHAR(100),
    discount_applied NUMERIC(10, 4)
        CHECK (
            discount_applied BETWEEN 0 AND 100
            OR discount_applied IS NULL
        ),
    total_amount NUMERIC(18, 2) NOT NULL
        CHECK (total_amount >= 0),
    payment_method VARCHAR(100),
    FOREIGN KEY (product_id)
        REFERENCES warehouse.dim_product(product_id),
    FOREIGN KEY (store_id)
        REFERENCES warehouse.dim_store(store_id)
);


-- ============================================================
-- Like-for-like store analysis
-- ============================================================

CREATE TABLE IF NOT EXISTS warehouse.dim_lfl_store (
    year INTEGER NOT NULL,
    month INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),
    store_id VARCHAR(100) NOT NULL,
    lfl_status BOOLEAN NOT NULL,
    PRIMARY KEY (year, month, store_id),
    FOREIGN KEY (store_id)
        REFERENCES warehouse.dim_store(store_id)
);


-- ============================================================
-- Daily store-product analytics
--
-- Grain:
--   one row per sales date, store, and product.
-- ============================================================

CREATE TABLE IF NOT EXISTS analytics.daily_store_product_sales (
    sales_date DATE NOT NULL,
    store_id VARCHAR(100) NOT NULL,
    product_id VARCHAR(100) NOT NULL,
    total_quantity NUMERIC(18, 2) NOT NULL,
    total_revenue NUMERIC(18, 2) NOT NULL,
    total_transaction_count INTEGER NOT NULL,
    unique_customers INTEGER NOT NULL,
    weighted_discount_pct NUMERIC(12, 4),
    PRIMARY KEY (
        sales_date,
        store_id,
        product_id
    ),
    FOREIGN KEY (store_id)
        REFERENCES warehouse.dim_store(store_id),
    FOREIGN KEY (product_id)
        REFERENCES warehouse.dim_product(product_id)
);


-- ============================================================
-- Indexes for common joins and analytical filters
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_fact_sales_store_id
    ON warehouse.fact_sales (store_id);

CREATE INDEX IF NOT EXISTS idx_fact_sales_product_id
    ON warehouse.fact_sales (product_id);

CREATE INDEX IF NOT EXISTS idx_fact_sales_transaction_date
    ON warehouse.fact_sales (transaction_date);

CREATE INDEX IF NOT EXISTS idx_fact_sales_store_date
    ON warehouse.fact_sales (store_id, transaction_date);

CREATE INDEX IF NOT EXISTS idx_fact_sales_product_date
    ON warehouse.fact_sales (product_id, transaction_date);

CREATE INDEX IF NOT EXISTS idx_lfl_store_store_month
    ON warehouse.dim_lfl_store (store_id, year, month);