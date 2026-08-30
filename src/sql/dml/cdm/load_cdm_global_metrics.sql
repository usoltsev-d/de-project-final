INSERT INTO cdm.global_metrics_shadow
(
    date_update,
    currency_from,
    amount_total,
    cnt_transactions,
    avg_transactions_per_account,
    cnt_accounts_make_transactions
)
-- Агрегация успешно проведённых транзакций по дате и валюте.
WITH transactions_agg AS
(
    SELECT
        toDate(transaction_dt) AS date_update,
        currency_code AS currency_from,

        -- Денежный оборот считаем по модулю, чтобы входящие и исходящие операции не взаимопогашались.
        -- amount хранится в минимальной единице валюты, поэтому делим на 100.
        sum(abs(amount)) / 100.0 AS amount_in_currency,

        -- Общее количество успешно проведённых транзакций.
        count() AS cnt_transactions,

        -- Количество уникальных счетов отправителей.
        uniqExact(account_number_from) AS cnt_accounts_make_transactions
    FROM dds.transactions
    WHERE transaction_dt >= toDateTime64({process_date:Date}, 3, 'UTC')
    AND transaction_dt <  toDateTime64({process_date:Date} + INTERVAL 1 DAY, 3, 'UTC')
    AND status = 'done' -- Транзакция проведена успешно
    GROUP BY
        date_update,
        currency_from
)

SELECT
    t.date_update, -- Дата расчёта
    t.currency_from, -- Код валюты транзакции

     -- Общий денежный оборот компании в USD
    round(
        t.amount_in_currency
        * if(
            t.currency_from = 420,
            1.0, -- Для USD дополнительный пересчёт не требуется
            toFloat64(c.currency_rate)
        ),
        2
    ) AS amount_total,

     -- Количество проведённых транзакций
    t.cnt_transactions,

     -- Среднее количество успешно проведённых транзакций на один уникальный счёт отправителя.
    if(
        t.cnt_accounts_make_transactions = 0,
        0.0,
        t.cnt_transactions / t.cnt_accounts_make_transactions
    ) AS avg_transactions_per_account,

    --  Количество уникальных аккаунтов с совершёнными транзакциями по валюте
    t.cnt_accounts_make_transactions
FROM transactions_agg AS t
LEFT JOIN dds.currencies AS c
   ON c.rate_date = t.date_update
   AND c.currency_from = t.currency_from
   AND c.currency_to = 420; - Для расчёта amount_total нужны курсы всех валют относительно USD.
