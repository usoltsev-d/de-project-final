from pathlib import Path

from lib.clickhouse_client import ClickHouseClient


PROJECT_ROOT = Path(__file__).resolve().parents[3]


class StgRepository:
    def __init__(
        self,
        clickhouse: ClickHouseClient,
    ) -> None:
        self._clickhouse = clickhouse

        self._transactions_sql = (
            PROJECT_ROOT
            / "src"
            / "sql"
            / "dml"
            / "stg"
            / "01_load_transactions.sql"
        ).read_text(encoding="utf-8")

        self._currencies_sql = (
            PROJECT_ROOT
            / "src"
            / "sql"
            / "dml"
            / "stg"
            / "02_load_currencies.sql"
        ).read_text(encoding="utf-8")

    def get_max_raw_offset(
        self,
        kafka_topic: str,
        kafka_partition: int,
    ) -> int | None:
        result = self._clickhouse.client.query(
            """
            SELECT maxOrNull(kafka_offset)
            FROM raw.kafka_events
            WHERE kafka_topic = {kafka_topic:String}
            AND kafka_partition = {kafka_partition:UInt16}
            """,
            parameters={
                "kafka_topic": kafka_topic,
                "kafka_partition": kafka_partition,
            },
        )

        return result.first_row[0]

    def load_transactions(
        self,
        kafka_topic: str,
        kafka_partition: int,
        offset_from: int,
        offset_to: int,
    ) -> None:
        self._clickhouse.client.command(
            self._transactions_sql,
            parameters={
                "kafka_topic": kafka_topic,
                "kafka_partition": kafka_partition,
                "offset_from": offset_from,
                "offset_to": offset_to,
            },
        )

    def load_currencies(
        self,
        kafka_topic: str,
        kafka_partition: int,
        offset_from: int,
        offset_to: int,
    ) -> None:
        self._clickhouse.client.command(
            self._currencies_sql,
            parameters={
                "kafka_topic": kafka_topic,
                "kafka_partition": kafka_partition,
                "offset_from": offset_from,
                "offset_to": offset_to,
            },
        )

    def close(self) -> None:
        self._clickhouse.close()