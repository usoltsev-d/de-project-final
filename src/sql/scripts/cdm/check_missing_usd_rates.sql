SELECT
    t.currency_from
FROM
(
    SELECT DISTINCT
        currency_code AS currency_from
    FROM dds.transactions
    WHERE transaction_dt >= toDateTime64({process_date:Date}, 3, 'UTC')
    AND transaction_dt < toDateTime64(
                                        {process_date:Date} + INTERVAL 1 DAY,
                                        3,
                                        'UTC'
                                     )
    AND status = 'done'
    AND currency_code != 420 -- Для USD курс к USD не требуется
) AS t
LEFT ANTI JOIN
(
    SELECT DISTINCT
        currency_from
    FROM dds.currencies
    WHERE rate_date = {process_date:Date}
      AND currency_to = 420
) AS c
    ON c.currency_from = t.currency_from
ORDER BY t.currency_from;