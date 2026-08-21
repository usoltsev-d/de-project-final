from typing import Sequence

from lib.clickhouse_client import ClickHouseClient

class RawRepository:
    def __init__(
        self,
        clickhouse: ClickHouseClient,
    ) -> None:
        self._clickhouse = clickhouse

    def save_events(self, events: Sequence[tuple]) -> None:
        if not events:
            return

        self._clickhouse.client.insert(
            "raw.kafka_events",
            events,
            column_names=[
                "kafka_topic",
                "kafka_partition",
                "kafka_offset",
                "message",
            ],
        )

    def close(self) -> None:
        self._clickhouse.close()