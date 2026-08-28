CREATE TABLE IF NOT EXISTS cdm.global_metrics
(
    date_update Date,
    currency_from Int32,
    amount_total Decimal64(2),
    cnt_transactions UInt64,
    avg_transactions_per_account Float64,
    cnt_accounts_make_transactions UInt64
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(date_update)
ORDER BY (
    date_update,
    currency_from
);