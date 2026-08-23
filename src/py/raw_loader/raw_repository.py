from typing import Sequence

from lib.clickhouse_client import ClickHouseClient


class RawRepository:
    def __init__(
        self,
        clickhouse: ClickHouseClient,
    ) -> None:
        self._clickhouse = clickhouse

    def save_events(
        self,
        events: Sequence[tuple],
    ) -> int:
        if not events:
            return 0

        topic = events[0][0]
        partition = events[0][1]

        offsets = [
            event[2]
            for event in events
        ]

        offset_from = min(offsets)
        offset_to = max(offsets)

        # Получаем offsets из текущего диапазона batch,
        # которые уже были сохранены в RAW.
        existing_offsets = self._get_existing_offsets(
            topic=topic,
            partition=partition,
            offset_from=offset_from,
            offset_to=offset_to,
        )

        # Kafka offset однозначно идентифицирует сообщение
        # внутри пары topic + partition, поэтому уже сохранённые
        # offsets можно безопасно исключить из повторной вставки.
        new_events = [
            event
            for event in events
            if event[2] not in existing_offsets
        ]

        if not new_events:
            return 0

        self._clickhouse.client.insert(
            "raw.kafka_events",
            new_events,
            column_names=[
                "kafka_topic",
                "kafka_partition",
                "kafka_offset",
                "kafka_message",
            ],
        )

        return len(new_events)

    def _get_existing_offsets(
        self,
        topic: str,
        partition: int,
        offset_from: int,
        offset_to: int,
    ) -> set[int]:
        result = self._clickhouse.client.query(
            """
            SELECT kafka_offset
            FROM raw.kafka_events
            WHERE kafka_topic = {topic:String}
            AND kafka_partition = {partition:UInt16}
            AND kafka_offset >= {offset_from:UInt64}
            AND kafka_offset <= {offset_to:UInt64}
            """,
            parameters={
                "topic": topic,
                "partition": partition,
                "offset_from": offset_from,
                "offset_to": offset_to,
            },
        )

        return {
            row[0]
            for row in result.result_rows
        }

    def close(self) -> None:
        self._clickhouse.close()