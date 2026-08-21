INSERT INTO stg.currencies
(
    date_update,
    currency_code,
    currency_code_with,
    currency_with_div
)
WITH
    JSONExtractRaw(kafka_message, 'payload') AS payload
SELECT
    parseDateTime64BestEffort(
        JSONExtractString(payload, 'date_update'),
        3,
        'UTC'
    ),
    JSONExtractUInt(payload, 'currency_code'),
    JSONExtractUInt(payload, 'currency_code_with'),
    toDecimal64(
        JSONExtractRaw(payload, 'currency_with_div'),
        8
    )
FROM raw.kafka_events
WHERE kafka_topic = {kafka_topic:String}
AND kafka_partition = {kafka_partition:UInt16}
AND kafka_offset > {offset_from:Int64}
AND kafka_offset <= {offset_to:Int64}
AND JSONExtractString(kafka_message, 'object_type') = 'CURRENCY';