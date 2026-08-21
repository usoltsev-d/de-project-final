CREATE TABLE IF NOT EXISTS raw.stg_load_offsets
(
    object_type LowCardinality(String),
    kafka_topic LowCardinality(String),
    kafka_partition UInt16,
    last_offset Int64,
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (
    object_type,
    kafka_topic,
    kafka_partition
);