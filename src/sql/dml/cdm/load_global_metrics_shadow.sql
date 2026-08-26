INSERT INTO cdm.global_metrics_shadow
(
    date_update,
    currency_from,
    amount_total,
    cnt_transactions,
    avg_transactions_per_account,
    cnt_accounts_make_transactions
)
-- Агрегация успешно проведенных транзакций по дате и валюте
WITH transactions_agg AS
(
    SELECT
        toDate(transaction_dt) AS date_update,
        currency_code AS currency_from,
        -- Для расчёта денежного оборота суммы транзакций берём по модулю,
        -- чтобы входящие и исходящие операции не взаимопогашались.
        -- amount хранится в минимальной единице валюты, поэтому делим на 100.
        sum(abs(amount)) / 100.0 AS amount_in_currency,
        -- Общее количество проведённых транзакций.
        count() AS cnt_transactions,
        -- Количество проведённых транзакций, для которых известен счёт отправителя.
        countIf(account_number_from != -1) AS cnt_account_transactions,
         -- Количество уникальных известных счетов отправителей
        uniqExactIf(
            account_number_from,
            account_number_from != -1
        ) AS cnt_accounts_make_transactions
    FROM dds.fct_transactions
    WHERE transaction_dt >= toDateTime64({process_date:Date}, 3, 'UTC')
    AND transaction_dt <  toDateTime64({process_date:Date} + INTERVAL 1 DAY, 3, 'UTC')
    AND status = 'done' -- Транзакция проведена успешно
    GROUP BY
        date_update,
        currency_from
)
SELECT
    t.date_update,
    t.currency_from,
    -- Общий денежный оборот пересчитывается в USD.
    -- Если деньги были переведены в USD, то дополнительный пересчёт не нужен
    round(
        t.amount_in_currency
        * if(
            t.currency_from = 420,
            1.0,
            toFloat64(r.currency_rate)
        ),
        2
    ) AS amount_total,
    t.cnt_transactions,
    -- Среднее количество транзакций на один известный счёт отправителя.
    -- Не учитываются транзакции для которых неизвестен счет отправителя (account_number_from = -1)
    if(
        t.cnt_accounts_make_transactions = 0,
        0.0,
        t.cnt_account_transactions
            / t.cnt_accounts_make_transactions
    ) AS avg_transactions_per_account,
    t.cnt_accounts_make_transactions
FROM transactions_agg AS t
LEFT JOIN dds.fct_currency_rates AS r
    ON r.rate_date = t.date_update
    AND r.currency_from = t.currency_from
    AND r.currency_to = 420; -- Для расчёта amount_total нужны курсы всех валют относительно USD.