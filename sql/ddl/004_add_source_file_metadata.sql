-- ============================================================
-- Source File Version Metadata
-- ============================================================

ALTER TABLE metadata.pipeline_runs
ADD COLUMN IF NOT EXISTS source_hash VARCHAR(64);

ALTER TABLE metadata.pipeline_runs
ADD COLUMN IF NOT EXISTS source_size_bytes BIGINT;

ALTER TABLE metadata.pipeline_runs
ADD COLUMN IF NOT EXISTS source_modified_at TIMESTAMPTZ;


CREATE INDEX IF NOT EXISTS idx_pipeline_runs_source_version
ON metadata.pipeline_runs (
    pipeline_name,
    source_name,
    source_hash
)
WHERE status = 'SUCCESS';