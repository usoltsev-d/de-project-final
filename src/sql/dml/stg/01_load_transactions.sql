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
WITH
    JSONExtractRaw(kafka_message, 'payload') AS payload
SELECT
    toUUID(JSONExtractString(payload, 'operation_id')),
    JSONExtractUInt(payload, 'account_number_from'),
    JSONExtractUInt(payload, 'account_number_to'),
    JSONExtractUInt(payload, 'currency_code'),
    JSONExtractString(payload, 'country'),
    JSONExtractString(payload, 'status'),
    JSONExtractString(payload, 'transaction_type'),
    JSONExtractUInt(payload, 'amount'),
    parseDateTime64BestEffort(
        JSONExtractString(payload, 'transaction_dt'),
        3,
        'UTC'
    )
FROM raw.kafka_events
WHERE kafka_topic = {kafka_topic:String}
AND kafka_partition = {kafka_partition:UInt16}
AND kafka_offset > {offset_from:Int64}
AND kafka_offset <= {offset_to:Int64}
AND JSONExtractString(kafka_message, 'object_type') = 'TRANSACTION';