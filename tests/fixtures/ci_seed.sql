-- ============================================================
-- FMCG CI SAMPLE DATA
-- Minimal relationally-consistent dataset for dbt build
-- ============================================================


-- ------------------------------------------------------------
-- SALES PIPELINE RUN
-- ------------------------------------------------------------

INSERT INTO metadata.pipeline_runs (
    pipeline_name,
    source_name,
    started_at,
    finished_at,
    records_received,
    records_inserted,
    records_rejected,
    status,
    source_hash
)
VALUES (
    'fact_sales_ingestion',
    'FactSales_CI.csv',
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP,
    1,
    1,
    0,
    'SUCCESS',
    repeat('a', 64)
)
RETURNING run_id
\gset sales_


INSERT INTO raw.sales (
    date,
    distributor_id,
    salesperson_id,
    outlet_id,
    product_id,
    invoice_id,
    quantity,
    unit_price,
    discount_pct,
    discount_amount,
    gross_sales,
    net_sales,
    unit_cogs,
    total_cogs,
    source_file,
    source_row_number,
    pipeline_run_id
)
VALUES (
    '2026-09-01',
    'DST001',
    'SP001',
    'OUT001',
    'P001',
    'INV_CI_001',
    '2',
    '10000',
    '0.1000',
    '2000',
    '20000',
    '18000',
    '6000',
    '12000',
    'FactSales_CI.csv',
    2,
    :sales_run_id
);


INSERT INTO metadata.source_state (
    pipeline_name,
    source_name,
    active_run_id,
    active_source_hash
)
VALUES (
    'fact_sales_ingestion',
    'FactSales_CI.csv',
    :sales_run_id,
    repeat('a', 64)
);



-- ------------------------------------------------------------
-- PRODUCT
-- ------------------------------------------------------------

INSERT INTO metadata.pipeline_runs (
    pipeline_name,
    source_name,
    status,
    source_hash
)
VALUES (
    'master_data_ingestion',
    'KopDes_MasterData.xlsx:DimProduct',
    'SUCCESS',
    repeat('b', 64)
)
RETURNING run_id
\gset product_


INSERT INTO raw.product (
    product_id,
    category,
    brand,
    product_name,
    variant,
    pack_size,
    unit_price,
    unit_cogs,
    source_file,
    source_sheet,
    source_row_number,
    pipeline_run_id
)
VALUES (
    'P001',
    'Beverage',
    'CI Brand',
    'CI Product',
    'Original',
    '1 PCS',
    '10000',
    '6000',
    'KopDes_MasterData.xlsx',
    'DimProduct',
    2,
    :product_run_id
);


INSERT INTO metadata.source_state
VALUES (
    'master_data_ingestion',
    'KopDes_MasterData.xlsx:DimProduct',
    :product_run_id,
    repeat('b', 64),
    CURRENT_TIMESTAMP
);



-- ------------------------------------------------------------
-- DISTRIBUTOR
-- ------------------------------------------------------------

INSERT INTO metadata.pipeline_runs (
    pipeline_name,
    source_name,
    status,
    source_hash
)
VALUES (
    'master_data_ingestion',
    'KopDes_MasterData.xlsx:DimDistributor',
    'SUCCESS',
    repeat('c', 64)
)
RETURNING run_id
\gset distributor_


INSERT INTO raw.distributor (
    distributor_id,
    distributor_name,
    region,
    province,
    city,
    distributor_type,
    start_year,
    source_file,
    source_sheet,
    source_row_number,
    pipeline_run_id
)
VALUES (
    'DST001',
    'CI Distributor',
    'Java',
    'DKI Jakarta',
    'Jakarta',
    'Main Distributor',
    '2020',
    'KopDes_MasterData.xlsx',
    'DimDistributor',
    2,
    :distributor_run_id
);


INSERT INTO metadata.source_state
VALUES (
    'master_data_ingestion',
    'KopDes_MasterData.xlsx:DimDistributor',
    :distributor_run_id,
    repeat('c', 64),
    CURRENT_TIMESTAMP
);



-- ------------------------------------------------------------
-- SALESPERSON
-- ------------------------------------------------------------

INSERT INTO metadata.pipeline_runs (
    pipeline_name,
    source_name,
    status,
    source_hash
)
VALUES (
    'master_data_ingestion',
    'KopDes_MasterData.xlsx:DimSalesperson',
    'SUCCESS',
    repeat('d', 64)
)
RETURNING run_id
\gset salesperson_


INSERT INTO raw.salesperson (
    salesperson_id,
    salesperson_name,
    supervisor,
    distributor_id,
    territory,
    join_year,
    source_file,
    source_sheet,
    source_row_number,
    pipeline_run_id
)
VALUES (
    'SP001',
    'CI Salesperson',
    'CI Supervisor',
    'DST001',
    'Jakarta',
    '2024',
    'KopDes_MasterData.xlsx',
    'DimSalesperson',
    2,
    :salesperson_run_id
);


INSERT INTO metadata.source_state
VALUES (
    'master_data_ingestion',
    'KopDes_MasterData.xlsx:DimSalesperson',
    :salesperson_run_id,
    repeat('d', 64),
    CURRENT_TIMESTAMP
);



-- ------------------------------------------------------------
-- OUTLET
-- ------------------------------------------------------------

INSERT INTO metadata.pipeline_runs (
    pipeline_name,
    source_name,
    status,
    source_hash
)
VALUES (
    'master_data_ingestion',
    'KopDes_MasterData.xlsx:DimOutlet',
    'SUCCESS',
    repeat('e', 64)
)
RETURNING run_id
\gset outlet_


INSERT INTO raw.outlet (
    outlet_id,
    outlet_name,
    channel,
    outlet_tier,
    distributor_id,
    salesperson_id,
    city,
    province,
    region,
    open_year,
    source_file,
    source_sheet,
    source_row_number,
    pipeline_run_id
)
VALUES (
    'OUT001',
    'CI Outlet',
    'General Trade',
    'A',
    'DST001',
    'SP001',
    'Jakarta',
    'DKI Jakarta',
    'Java',
    '2024',
    'KopDes_MasterData.xlsx',
    'DimOutlet',
    2,
    :outlet_run_id
);


INSERT INTO metadata.source_state
VALUES (
    'master_data_ingestion',
    'KopDes_MasterData.xlsx:DimOutlet',
    :outlet_run_id,
    repeat('e', 64),
    CURRENT_TIMESTAMP
);



-- ------------------------------------------------------------
-- TARGET
-- ------------------------------------------------------------

INSERT INTO metadata.pipeline_runs (
    pipeline_name,
    source_name,
    status,
    source_hash
)
VALUES (
    'master_data_ingestion',
    'KopDes_MasterData.xlsx:FactTarget',
    'SUCCESS',
    repeat('f', 64)
)
RETURNING run_id
\gset target_


INSERT INTO raw.target (
    month_start,
    salesperson_id,
    target_sales,
    target_active_outlets,
    source_file,
    source_sheet,
    source_row_number,
    pipeline_run_id
)
VALUES (
    '2026-09-01',
    'SP001',
    '50000',
    '1',
    'KopDes_MasterData.xlsx',
    'FactTarget',
    2,
    :target_run_id
);


INSERT INTO metadata.source_state
VALUES (
    'master_data_ingestion',
    'KopDes_MasterData.xlsx:FactTarget',
    :target_run_id,
    repeat('f', 64),
    CURRENT_TIMESTAMP
);



-- ------------------------------------------------------------
-- INVENTORY
-- ------------------------------------------------------------

INSERT INTO metadata.pipeline_runs (
    pipeline_name,
    source_name,
    status,
    source_hash
)
VALUES (
    'master_data_ingestion',
    'KopDes_MasterData.xlsx:FactInventory',
    'SUCCESS',
    repeat('1', 64)
)
RETURNING run_id
\gset inventory_


INSERT INTO raw.inventory (
    month_start,
    distributor_id,
    product_id,
    opening_stock,
    stock_received,
    units_sold,
    closing_stock,
    stock_value,
    source_file,
    source_sheet,
    source_row_number,
    pipeline_run_id
)
VALUES (
    '2026-09-01',
    'DST001',
    'P001',
    '10',
    '5',
    '2',
    '13',
    '78000',
    'KopDes_MasterData.xlsx',
    'FactInventory',
    2,
    :inventory_run_id
);


INSERT INTO metadata.source_state
VALUES (
    'master_data_ingestion',
    'KopDes_MasterData.xlsx:FactInventory',
    :inventory_run_id,
    repeat('1', 64),
    CURRENT_TIMESTAMP
);