CREATE TABLE IF NOT EXISTS raw.transaction_service_events
(
    object_id UUID,
    object_type LowCardinality(String),
    sent_dttm DateTime64(3, 'UTC'),
    payload String,
    created_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(sent_dttm)
ORDER BY (sent_dttm, object_type, object_id);