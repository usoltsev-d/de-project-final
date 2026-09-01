INSERT INTO dds.dim_currency
(
    currency_code,
    country,
    currency_iso_code
)
SELECT DISTINCT
    currency_code,
    country,
    CASE country
        WHEN 'canada'  THEN 'CAD'
        WHEN 'usa'     THEN 'USD'
        WHEN 'russia'  THEN 'RUB'
        WHEN 'italy'   THEN 'EUR'
        WHEN 'china'   THEN 'CNY'
        WHEN 'england' THEN 'GBP'
        WHEN 'turkey'  THEN 'TRY'
        ELSE 'UNKNOWN'
    END AS currency_iso_code
FROM dds.transactions
WHERE transaction_dt >= toDateTime64({process_date:Date}, 3, 'UTC')
AND transaction_dt <  toDateTime64({process_date:Date} + INTERVAL 1 DAY, 3, 'UTC')
AND currency_code NOT IN
(
    SELECT currency_code
    FROM dds.dim_currency
);