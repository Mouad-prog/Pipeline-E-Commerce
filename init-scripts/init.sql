-- E-Commerce Data Pipeline Schema

-- Raw transactions table
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id   VARCHAR(64) PRIMARY KEY,
    client_id        VARCHAR(64) NOT NULL,
    product_id       VARCHAR(64) NOT NULL,
    quantity         INTEGER NOT NULL,
    unit_price       NUMERIC(12, 2) NOT NULL,
    total_amount     NUMERIC(12, 2) NOT NULL,
    currency         VARCHAR(3) NOT NULL DEFAULT 'USD',
    payment_method   VARCHAR(32) NOT NULL,
    status           VARCHAR(16) NOT NULL,
    transaction_time TIMESTAMP NOT NULL,
    ingested_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Raw clients table
CREATE TABLE IF NOT EXISTS clients (
    client_id    VARCHAR(64) PRIMARY KEY,
    first_name   VARCHAR(128) NOT NULL,
    last_name    VARCHAR(128) NOT NULL,
    email        VARCHAR(256) NOT NULL,
    country      VARCHAR(64) NOT NULL,
    city         VARCHAR(128) NOT NULL,
    age          INTEGER NOT NULL,
    segment      VARCHAR(32) NOT NULL,
    registered_at TIMESTAMP NOT NULL,
    ingested_at  TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Raw products table
CREATE TABLE IF NOT EXISTS products (
    product_id   VARCHAR(64) PRIMARY KEY,
    name         VARCHAR(256) NOT NULL,
    category     VARCHAR(128) NOT NULL,
    sub_category VARCHAR(128),
    brand        VARCHAR(128) NOT NULL,
    price        NUMERIC(12, 2) NOT NULL,
    weight_kg    NUMERIC(8, 3),
    rating       NUMERIC(3, 2),
    stock        INTEGER NOT NULL DEFAULT 0,
    ingested_at  TIMESTAMP NOT NULL DEFAULT NOW()
);

-- KPI: Revenue by category
CREATE TABLE IF NOT EXISTS kpi_revenue_by_category (
    window_start TIMESTAMP NOT NULL,
    window_end   TIMESTAMP NOT NULL,
    category     VARCHAR(128) NOT NULL,
    total_revenue NUMERIC(14, 2) NOT NULL,
    order_count  INTEGER NOT NULL,
    avg_order_value NUMERIC(12, 2) NOT NULL,
    computed_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (window_start, window_end, category)
);

-- KPI: Revenue by country
CREATE TABLE IF NOT EXISTS kpi_revenue_by_country (
    window_start  TIMESTAMP NOT NULL,
    window_end    TIMESTAMP NOT NULL,
    country       VARCHAR(64) NOT NULL,
    total_revenue NUMERIC(14, 2) NOT NULL,
    order_count   INTEGER NOT NULL,
    unique_clients INTEGER NOT NULL,
    computed_at   TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (window_start, window_end, country)
);

-- KPI: Top products
CREATE TABLE IF NOT EXISTS kpi_top_products (
    window_start  TIMESTAMP NOT NULL,
    window_end    TIMESTAMP NOT NULL,
    product_id    VARCHAR(64) NOT NULL,
    product_name  VARCHAR(256) NOT NULL,
    units_sold    INTEGER NOT NULL,
    total_revenue NUMERIC(14, 2) NOT NULL,
    computed_at   TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (window_start, window_end, product_id)
);

-- KPI: Customer segments
CREATE TABLE IF NOT EXISTS kpi_customer_segments (
    window_start    TIMESTAMP NOT NULL,
    window_end      TIMESTAMP NOT NULL,
    segment         VARCHAR(32) NOT NULL,
    total_revenue   NUMERIC(14, 2) NOT NULL,
    order_count     INTEGER NOT NULL,
    unique_clients  INTEGER NOT NULL,
    avg_spend       NUMERIC(12, 2) NOT NULL,
    computed_at     TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (window_start, window_end, segment)
);

-- Pipeline run metadata
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          VARCHAR(64) PRIMARY KEY,
    start_time      TIMESTAMP NOT NULL,
    end_time        TIMESTAMP,
    status          VARCHAR(16) NOT NULL DEFAULT 'RUNNING',
    records_ingested INTEGER DEFAULT 0,
    records_processed INTEGER DEFAULT 0,
    records_stored  INTEGER DEFAULT 0,
    errors          INTEGER DEFAULT 0,
    notes           TEXT
);

-- Indexes for query performance
CREATE INDEX IF NOT EXISTS idx_transactions_client ON transactions(client_id);
CREATE INDEX IF NOT EXISTS idx_transactions_product ON transactions(product_id);
CREATE INDEX IF NOT EXISTS idx_transactions_time ON transactions(transaction_time);
CREATE INDEX IF NOT EXISTS idx_kpi_rev_cat_window ON kpi_revenue_by_category(window_start, window_end);
CREATE INDEX IF NOT EXISTS idx_kpi_rev_country_window ON kpi_revenue_by_country(window_start, window_end);
CREATE INDEX IF NOT EXISTS idx_kpi_top_prod_window ON kpi_top_products(window_start, window_end);
