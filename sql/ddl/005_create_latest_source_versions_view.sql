CREATE OR REPLACE VIEW metadata.latest_source_versions AS

SELECT
    pipeline_name,
    source_name,
    run_id,
    source_hash,
    source_size_bytes,
    source_modified_at,
    started_at,
    finished_at
FROM (
    SELECT
        run_id,
        pipeline_name,
        source_name,
        source_hash,
        source_size_bytes,
        source_modified_at,
        started_at,
        finished_at,

        ROW_NUMBER() OVER (
            PARTITION BY
                pipeline_name,
                source_name
            ORDER BY
                finished_at DESC,
                run_id DESC
        ) AS version_rank

    FROM metadata.pipeline_runs

    WHERE status = 'SUCCESS'
) ranked

WHERE version_rank = 1;