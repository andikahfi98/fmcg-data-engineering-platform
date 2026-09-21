ALTER TABLE metadata.batch_pipeline_audit
ADD COLUMN IF NOT EXISTS raw_streaming_sales_rows BIGINT;

ALTER TABLE metadata.batch_pipeline_audit
ADD COLUMN IF NOT EXISTS staging_streaming_sales_rows BIGINT;

ALTER TABLE metadata.batch_pipeline_audit
ADD COLUMN IF NOT EXISTS unified_sales_rows BIGINT;