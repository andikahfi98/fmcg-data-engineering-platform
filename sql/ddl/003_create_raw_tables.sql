-- ============================================================
-- FMCG Batch & Streaming Data Engineering Platform
-- RAW Layer Tables
-- ============================================================


-- ============================================================
-- SALES
-- Grain: one source sales row
-- ============================================================

CREATE TABLE IF NOT EXISTS raw.sales (
    raw_record_id BIGSERIAL PRIMARY KEY,

    date TEXT,
    distributor_id TEXT,
    salesperson_id TEXT,
    outlet_id TEXT,
    product_id TEXT,
    invoice_id TEXT,
    quantity TEXT,
    unit_price TEXT,
    discount_pct TEXT,
    discount_amount TEXT,
    gross_sales TEXT,
    net_sales TEXT,
    unit_cogs TEXT,
    total_cogs TEXT,

    source_file TEXT NOT NULL,
    source_row_number BIGINT,
    pipeline_run_id BIGINT NOT NULL
        REFERENCES metadata.pipeline_runs(run_id),

    ingested_at TIMESTAMPTZ NOT NULL
        DEFAULT CURRENT_TIMESTAMP
);


-- ============================================================
-- PRODUCT MASTER
-- Source: DimProduct
-- ============================================================

CREATE TABLE IF NOT EXISTS raw.product (
    raw_record_id BIGSERIAL PRIMARY KEY,

    product_id TEXT,
    category TEXT,
    brand TEXT,
    product_name TEXT,
    variant TEXT,
    pack_size TEXT,
    unit_price TEXT,
    unit_cogs TEXT,

    source_file TEXT NOT NULL,
    source_sheet TEXT NOT NULL,
    source_row_number BIGINT,
    pipeline_run_id BIGINT NOT NULL
        REFERENCES metadata.pipeline_runs(run_id),

    ingested_at TIMESTAMPTZ NOT NULL
        DEFAULT CURRENT_TIMESTAMP
);


-- ============================================================
-- DISTRIBUTOR MASTER
-- Source: DimDistributor
-- ============================================================

CREATE TABLE IF NOT EXISTS raw.distributor (
    raw_record_id BIGSERIAL PRIMARY KEY,

    distributor_id TEXT,
    distributor_name TEXT,
    region TEXT,
    province TEXT,
    city TEXT,
    distributor_type TEXT,
    start_year TEXT,

    source_file TEXT NOT NULL,
    source_sheet TEXT NOT NULL,
    source_row_number BIGINT,
    pipeline_run_id BIGINT NOT NULL
        REFERENCES metadata.pipeline_runs(run_id),

    ingested_at TIMESTAMPTZ NOT NULL
        DEFAULT CURRENT_TIMESTAMP
);


-- ============================================================
-- SALESPERSON MASTER
-- Source: DimSalesperson
-- ============================================================

CREATE TABLE IF NOT EXISTS raw.salesperson (
    raw_record_id BIGSERIAL PRIMARY KEY,

    salesperson_id TEXT,
    salesperson_name TEXT,
    supervisor TEXT,
    distributor_id TEXT,
    territory TEXT,
    join_year TEXT,

    source_file TEXT NOT NULL,
    source_sheet TEXT NOT NULL,
    source_row_number BIGINT,
    pipeline_run_id BIGINT NOT NULL
        REFERENCES metadata.pipeline_runs(run_id),

    ingested_at TIMESTAMPTZ NOT NULL
        DEFAULT CURRENT_TIMESTAMP
);


-- ============================================================
-- OUTLET MASTER
-- Source: DimOutlet
-- ============================================================

CREATE TABLE IF NOT EXISTS raw.outlet (
    raw_record_id BIGSERIAL PRIMARY KEY,

    outlet_id TEXT,
    outlet_name TEXT,
    channel TEXT,
    outlet_tier TEXT,
    distributor_id TEXT,
    salesperson_id TEXT,
    city TEXT,
    province TEXT,
    region TEXT,
    open_year TEXT,

    source_file TEXT NOT NULL,
    source_sheet TEXT NOT NULL,
    source_row_number BIGINT,
    pipeline_run_id BIGINT NOT NULL
        REFERENCES metadata.pipeline_runs(run_id),

    ingested_at TIMESTAMPTZ NOT NULL
        DEFAULT CURRENT_TIMESTAMP
);


-- ============================================================
-- SALES TARGET
-- Expected grain: MonthStart + SalespersonID
-- ============================================================

CREATE TABLE IF NOT EXISTS raw.target (
    raw_record_id BIGSERIAL PRIMARY KEY,

    month_start TEXT,
    salesperson_id TEXT,
    target_sales TEXT,
    target_active_outlets TEXT,

    source_file TEXT NOT NULL,
    source_sheet TEXT NOT NULL,
    source_row_number BIGINT,
    pipeline_run_id BIGINT NOT NULL
        REFERENCES metadata.pipeline_runs(run_id),

    ingested_at TIMESTAMPTZ NOT NULL
        DEFAULT CURRENT_TIMESTAMP
);


-- ============================================================
-- INVENTORY
-- Expected grain: MonthStart + DistributorID + ProductID
-- ============================================================

CREATE TABLE IF NOT EXISTS raw.inventory (
    raw_record_id BIGSERIAL PRIMARY KEY,

    month_start TEXT,
    distributor_id TEXT,
    product_id TEXT,
    opening_stock TEXT,
    stock_received TEXT,
    units_sold TEXT,
    closing_stock TEXT,
    stock_value TEXT,

    source_file TEXT NOT NULL,
    source_sheet TEXT NOT NULL,
    source_row_number BIGINT,
    pipeline_run_id BIGINT NOT NULL
        REFERENCES metadata.pipeline_runs(run_id),

    ingested_at TIMESTAMPTZ NOT NULL
        DEFAULT CURRENT_TIMESTAMP
);