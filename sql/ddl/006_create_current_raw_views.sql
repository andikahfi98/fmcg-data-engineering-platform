CREATE OR REPLACE VIEW raw.current_sales AS

SELECT s.*
FROM raw.sales s

INNER JOIN metadata.latest_source_versions v
    ON s.pipeline_run_id = v.run_id

WHERE v.pipeline_name = 'fact_sales_ingestion';

CREATE OR REPLACE VIEW raw.current_product AS

SELECT p.*
FROM raw.product p

INNER JOIN metadata.latest_source_versions v
    ON p.pipeline_run_id = v.run_id

WHERE v.pipeline_name = 'master_data_ingestion';


CREATE OR REPLACE VIEW raw.current_distributor AS

SELECT d.*
FROM raw.distributor d

INNER JOIN metadata.latest_source_versions v
    ON d.pipeline_run_id = v.run_id

WHERE v.pipeline_name = 'master_data_ingestion';


CREATE OR REPLACE VIEW raw.current_salesperson AS

SELECT s.*
FROM raw.salesperson s

INNER JOIN metadata.latest_source_versions v
    ON s.pipeline_run_id = v.run_id

WHERE v.pipeline_name = 'master_data_ingestion';


CREATE OR REPLACE VIEW raw.current_outlet AS

SELECT o.*
FROM raw.outlet o

INNER JOIN metadata.latest_source_versions v
    ON o.pipeline_run_id = v.run_id

WHERE v.pipeline_name = 'master_data_ingestion';


CREATE OR REPLACE VIEW raw.current_target AS

SELECT t.*
FROM raw.target t

INNER JOIN metadata.latest_source_versions v
    ON t.pipeline_run_id = v.run_id

WHERE v.pipeline_name = 'master_data_ingestion';


CREATE OR REPLACE VIEW raw.current_inventory AS

SELECT i.*
FROM raw.inventory i

INNER JOIN metadata.latest_source_versions v
    ON i.pipeline_run_id = v.run_id

WHERE v.pipeline_name = 'master_data_ingestion';