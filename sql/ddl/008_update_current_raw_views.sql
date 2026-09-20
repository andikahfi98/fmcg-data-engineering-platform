CREATE OR REPLACE VIEW raw.current_sales AS
SELECT s.*
FROM raw.sales s
INNER JOIN metadata.source_state state
    ON s.pipeline_run_id = state.active_run_id
WHERE state.pipeline_name = 'fact_sales_ingestion';


CREATE OR REPLACE VIEW raw.current_product AS
SELECT p.*
FROM raw.product p
INNER JOIN metadata.source_state state
    ON p.pipeline_run_id = state.active_run_id
WHERE state.pipeline_name = 'master_data_ingestion'
  AND state.source_name = 'KopDes_MasterData.xlsx:DimProduct';


CREATE OR REPLACE VIEW raw.current_distributor AS
SELECT d.*
FROM raw.distributor d
INNER JOIN metadata.source_state state
    ON d.pipeline_run_id = state.active_run_id
WHERE state.pipeline_name = 'master_data_ingestion'
  AND state.source_name = 'KopDes_MasterData.xlsx:DimDistributor';


CREATE OR REPLACE VIEW raw.current_salesperson AS
SELECT s.*
FROM raw.salesperson s
INNER JOIN metadata.source_state state
    ON s.pipeline_run_id = state.active_run_id
WHERE state.pipeline_name = 'master_data_ingestion'
  AND state.source_name = 'KopDes_MasterData.xlsx:DimSalesperson';


CREATE OR REPLACE VIEW raw.current_outlet AS
SELECT o.*
FROM raw.outlet o
INNER JOIN metadata.source_state state
    ON o.pipeline_run_id = state.active_run_id
WHERE state.pipeline_name = 'master_data_ingestion'
  AND state.source_name = 'KopDes_MasterData.xlsx:DimOutlet';


CREATE OR REPLACE VIEW raw.current_target AS
SELECT t.*
FROM raw.target t
INNER JOIN metadata.source_state state
    ON t.pipeline_run_id = state.active_run_id
WHERE state.pipeline_name = 'master_data_ingestion'
  AND state.source_name = 'KopDes_MasterData.xlsx:FactTarget';


CREATE OR REPLACE VIEW raw.current_inventory AS
SELECT i.*
FROM raw.inventory i
INNER JOIN metadata.source_state state
    ON i.pipeline_run_id = state.active_run_id
WHERE state.pipeline_name = 'master_data_ingestion'
  AND state.source_name = 'KopDes_MasterData.xlsx:FactInventory';