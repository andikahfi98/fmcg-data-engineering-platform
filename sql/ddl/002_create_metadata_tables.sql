CREATE TABLE IF NOT EXISTS metadata.pipeline_runs (
    run_id BIGSERIAL PRIMARY KEY,
    pipeline_name VARCHAR(100) NOT NULL,
    source_name VARCHAR(100),
    started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMPTZ,
    records_received BIGINT NOT NULL DEFAULT 0,
    records_inserted BIGINT NOT NULL DEFAULT 0,
    records_rejected BIGINT NOT NULL DEFAULT 0,
    status VARCHAR(20) NOT NULL DEFAULT 'RUNNING',
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS metadata.watermarks (
    pipeline_name VARCHAR(100) PRIMARY KEY,
    watermark_column VARCHAR(100) NOT NULL,
    watermark_value VARCHAR(255),
    last_updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);