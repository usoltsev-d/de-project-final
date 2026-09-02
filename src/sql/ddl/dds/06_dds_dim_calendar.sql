CREATE TABLE IF NOT EXISTS dds.dim_calendar
(
    date Date,
    year UInt16,
    quarter UInt8,
    month UInt8
)
ENGINE = MergeTree
ORDER BY date;