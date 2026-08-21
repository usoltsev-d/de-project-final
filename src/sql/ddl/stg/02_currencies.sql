CREATE TABLE IF NOT EXISTS stg.currencies
(
    date_update DateTime64(3, 'UTC'),
    currency_code UInt32,
    currency_code_with UInt32,
    currency_with_div Decimal64(8)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date_update)
ORDER BY (date_update, currency_code, currency_code_with);