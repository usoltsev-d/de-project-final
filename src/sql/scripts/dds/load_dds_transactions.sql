INSERT INTO dds.transactions_shadow
(
    operation_id,
    account_number_from,
    account_number_to,
    currency_code,
    country,
    status,
    transaction_type,
    amount,
    transaction_dt
)
SELECT DISTINCT
    operation_id,
    account_number_from,
    account_number_to,
    currency_code,
    country,
    status,
    transaction_type,
    amount,
    transaction_dt
FROM stg.transactions
WHERE transaction_dt >= toDateTime64({process_date:Date}, 3, 'UTC')
AND transaction_dt <  toDateTime64({process_date:Date} + INTERVAL 1 DAY, 3, 'UTC')
AND account_number_from >= 0
AND account_number_to >= 0; -- Очистка от тестовых аккаунтов