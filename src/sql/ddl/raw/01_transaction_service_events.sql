CREATE TABLE IF NOT EXISTS raw.kafka_events
(
    kafka_topic LowCardinality(String),
    kafka_partition UInt16,
    kafka_offset UInt64,
    kafka_message String,
    created_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (
    kafka_topic,
    kafka_partition,
    kafka_offset
);