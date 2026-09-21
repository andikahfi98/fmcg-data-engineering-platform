CREATE TABLE IF NOT EXISTS raw.sales_streaming_events (
    streaming_record_id BIGSERIAL PRIMARY KEY,

    event_id VARCHAR(36) NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,

    source VARCHAR(100),
    source_file VARCHAR(255),

    payload JSONB NOT NULL,

    kafka_topic VARCHAR(255) NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,

    ingested_at TIMESTAMPTZ NOT NULL
        DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uq_sales_streaming_event_id
        UNIQUE (event_id),

    CONSTRAINT uq_sales_streaming_kafka_position
        UNIQUE (
            kafka_topic,
            kafka_partition,
            kafka_offset
        )
);