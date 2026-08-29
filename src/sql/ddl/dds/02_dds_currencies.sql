CREATE TABLE IF NOT EXISTS dds.currencies
(
    rate_date Date,
    currency_from Int32,
    currency_to Int32,
    currency_rate Decimal64(8),
    load_dttm DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(rate_date)
ORDER BY rate_date;