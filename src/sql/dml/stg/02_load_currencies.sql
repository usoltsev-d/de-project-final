INSERT INTO stg.currencies
(
    date_update,
    currency_code,
    currency_code_with,
    currency_with_div
)
WITH JSONExtract(
    kafka_message,
    'Tuple(
        object_type String,
        payload Tuple(
            date_update String,
            currency_code Int32,
            currency_code_with Int32,
            currency_with_div Decimal64(8)
        )
    )'
) AS event
SELECT
    parseDateTime64BestEffort(
        event.payload.date_update,
        3,
        'UTC'
    ),
    event.payload.currency_code,
    event.payload.currency_code_with,
    event.payload.currency_with_div
FROM raw.kafka_events
WHERE kafka_topic = {kafka_topic:String}
AND kafka_partition = {kafka_partition:UInt16}
AND kafka_offset > {offset_from:Int64}
AND kafka_offset <= {offset_to:Int64}
AND event.object_type = 'CURRENCY';