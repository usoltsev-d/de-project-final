CREATE TABLE IF NOT EXISTS dds.dim_currency
(
    currency_code Int32,
    country LowCardinality(String),
    load_dttm DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY currency_code;