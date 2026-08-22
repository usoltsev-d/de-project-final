CREATE TABLE IF NOT EXISTS stg.currencies
(
    date_update DateTime64(3, 'UTC'),
    currency_code UInt32,
    currency_code_with UInt32,
    currency_with_div Decimal64(8),
    load_dttm DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(date_update)
ORDER BY date_update;