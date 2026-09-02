INSERT INTO dds.dim_calendar
(
    date,
    year,
    quarter,
    month
)
WITH
    toDate({date_from:String}) AS date_from,
    toDate({date_to:String}) AS date_to
SELECT
    date,
    toYear(date),
    toQuarter(date),
    toMonth(date)
FROM
(
    SELECT
        addDays(date_from, number) AS date
    FROM numbers(
        dateDiff('day', date_from, date_to) + 1
    )
)
WHERE date NOT IN
(
    SELECT date
    FROM dds.dim_calendar
);