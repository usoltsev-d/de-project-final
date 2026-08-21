CREATE TABLE IF NOT EXISTS raw.transaction_service_events
(
    object_id UUID,
    object_type LowCardinality(String),
    sent_dttm DateTime64(3, 'UTC'),
    event_dttm DateTime64(3, 'UTC'),
    payload String,
    created_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(event_dttm)
ORDER BY (event_dttm, object_type, object_id);