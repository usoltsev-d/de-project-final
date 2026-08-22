CREATE TABLE IF NOT EXISTS stg.transactions
(
    operation_id UUID,
    account_number_from UInt64,
    account_number_to UInt64,
    currency_code UInt32,
    country LowCardinality(String),
    status LowCardinality(String),
    transaction_type LowCardinality(String),
    amount UInt64,
    transaction_dt DateTime64(3, 'UTC'),
    load_dttm DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(transaction_dt)
ORDER BY transaction_dt;