INSERT INTO dds.fct_currency_rates
(
    rate_date,
    currency_from,
    currency_to,
    currency_rate
)
SELECT DISTINCT
    toDate(date_update) AS rate_date,
    currency_code AS currency_from,
    currency_code_with AS currency_to,
    currency_with_div AS currency_rate
FROM stg.currencies
WHERE date_update >= toDateTime64({process_date:Date}, 3, 'UTC')
AND date_update <  toDateTime64({process_date:Date} + INTERVAL 1 DAY, 3, 'UTC');