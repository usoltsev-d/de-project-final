CREATE TABLE IF NOT EXISTS raw.kafka_events
(
    kafka_topic LowCardinality(String),
    kafka_partition UInt16,
    kafka_offset UInt64,
    object_id UUID,
    object_type LowCardinality(String),
    event_dttm DateTime64(3, 'UTC'),
    kafka_message String,
    load_dttm DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(event_dttm)
ORDER BY (
    object_type,
    event_dttm
);