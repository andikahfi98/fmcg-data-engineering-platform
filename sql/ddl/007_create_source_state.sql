CREATE TABLE IF NOT EXISTS metadata.source_state (
    pipeline_name VARCHAR(100) NOT NULL,
    source_name VARCHAR(255) NOT NULL,

    active_run_id BIGINT NOT NULL
        REFERENCES metadata.pipeline_runs(run_id),

    active_source_hash VARCHAR(64) NOT NULL,

    last_observed_at TIMESTAMPTZ NOT NULL
        DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (
        pipeline_name,
        source_name
    )
);


INSERT INTO metadata.source_state (
    pipeline_name,
    source_name,
    active_run_id,
    active_source_hash,
    last_observed_at
)

SELECT DISTINCT ON (
    pipeline_name,
    source_name
)
    pipeline_name,
    source_name,
    run_id,
    source_hash,
    CURRENT_TIMESTAMP

FROM metadata.pipeline_runs

WHERE status = 'SUCCESS'
  AND source_hash IS NOT NULL

ORDER BY
    pipeline_name,
    source_name,
    finished_at DESC NULLS LAST,
    run_id DESC

ON CONFLICT (
    pipeline_name,
    source_name
)

DO UPDATE SET
    active_run_id = EXCLUDED.active_run_id,
    active_source_hash = EXCLUDED.active_source_hash,
    last_observed_at = EXCLUDED.last_observed_at;