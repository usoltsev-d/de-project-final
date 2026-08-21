INSERT INTO stg.transactions
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
WITH JSONExtract(
    kafka_message,
    'Tuple(
        object_type String,
        payload Tuple(
            operation_id UUID,
            account_number_from UInt64,
            account_number_to UInt64,
            currency_code UInt32,
            country String,
            status String,
            transaction_type String,
            amount UInt64,
            transaction_dt String
        )
    )'
) AS event
SELECT
    event.payload.operation_id,
    event.payload.account_number_from,
    event.payload.account_number_to,
    event.payload.currency_code,
    event.payload.country,
    event.payload.status,
    event.payload.transaction_type,
    event.payload.amount,
    parseDateTime64BestEffort(
        event.payload.transaction_dt,
        3,
        'UTC'
    )
FROM raw.kafka_events
WHERE kafka_topic = {kafka_topic:String}
AND kafka_partition = {kafka_partition:UInt16}
AND kafka_offset > {offset_from:Int64}
AND kafka_offset <= {offset_to:Int64}
AND event.object_type = 'TRANSACTION';