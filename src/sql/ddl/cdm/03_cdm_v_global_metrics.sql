CREATE VIEW IF NOT EXISTS cdm.v_global_metrics AS
SELECT
    c.date AS date_update,
    c.year,
    c.quarter,
    c.month,
    dc.currency_code AS currency_from,
    dc.currency_iso_code,
    dc.country,
    coalesce(
        gm.amount_total,
        toDecimal64(0, 2)
    ) AS amount_total,
    coalesce(
        gm.cnt_transactions,
        toUInt64(0)
    ) AS cnt_transactions,
    coalesce(
        gm.avg_transactions_per_account,
        0.0
    ) AS avg_transactions_per_account,
    coalesce(
        gm.cnt_accounts_make_transactions,
        toUInt64(0)
    ) AS cnt_accounts_make_transactions
FROM dds.dim_calendar AS c
CROSS JOIN dds.dim_currency AS dc
LEFT JOIN cdm.global_metrics AS gm
    ON gm.date_update = c.date
    AND gm.currency_from = dc.currency_code
WHERE c.date BETWEEN
(
    SELECT min(date_update)
    FROM cdm.global_metrics
)
AND
(
    SELECT max(date_update)
    FROM cdm.global_metrics
);