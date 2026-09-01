INSERT INTO dds.dim_currency
(
    currency_code,
    country
)
SELECT DISTINCT
    currency_code,
    country
FROM dds.transactions
WHERE transaction_dt >= toDateTime64({process_date:Date}, 3, 'UTC')
AND transaction_dt <  toDateTime64({process_date:Date} + INTERVAL 1 DAY, 3, 'UTC')
AND currency_code NOT IN
(
    SELECT currency_code
    FROM dds.dim_currency
);