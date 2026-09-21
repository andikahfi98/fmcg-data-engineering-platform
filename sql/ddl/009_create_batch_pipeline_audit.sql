CREATE TABLE IF NOT EXISTS metadata.batch_pipeline_audit (
    audit_id BIGSERIAL PRIMARY KEY,

    dag_id VARCHAR(100) NOT NULL,
    dag_run_id VARCHAR(255) NOT NULL,

    raw_sales_rows BIGINT,
    staging_sales_rows BIGINT,
    warehouse_sales_rows BIGINT,

    raw_inventory_rows BIGINT,
    staging_inventory_rows BIGINT,
    warehouse_inventory_rows BIGINT,

    raw_target_rows BIGINT,
    staging_target_rows BIGINT,
    warehouse_target_rows BIGINT,

    validation_status VARCHAR(20) NOT NULL,

    validation_message TEXT,

    validated_at TIMESTAMPTZ NOT NULL
        DEFAULT CURRENT_TIMESTAMP
);


CREATE INDEX IF NOT EXISTS
idx_batch_pipeline_audit_run
ON metadata.batch_pipeline_audit (
    dag_id,
    dag_run_id
);