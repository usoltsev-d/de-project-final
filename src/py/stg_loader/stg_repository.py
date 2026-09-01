from datetime import date
from pathlib import Path

class StgRepository:
    def __init__(
        self,
        client,
        sql_dir: Path,
    ) -> None:
        self._client = client

        self._transactions_sql = (
            sql_dir / "load_stg_transactions.sql"
        ).read_text(encoding="utf-8")

        self._currencies_sql = (
            sql_dir / "load_stg_currencies.sql"
        ).read_text(encoding="utf-8")

    def get_max_raw_offset(
        self,
        kafka_topic: str,
        kafka_partition: int,
    ) -> int | None:
        result = self._client.query(
            """
            SELECT kafka_offset
            FROM raw.kafka_events
            WHERE kafka_topic = {kafka_topic:String}
            AND kafka_partition = {kafka_partition:UInt16}
            ORDER BY kafka_offset DESC
            LIMIT 1
            """,
            parameters={
                "kafka_topic": kafka_topic,
                "kafka_partition": kafka_partition,
            },
        )

        if not result.result_rows:
            return None

        return result.first_row[0]

    def get_latest_transaction_date(self) -> date | None:
        result = self._client.query(
            """
            SELECT toDate(transaction_dt)
            FROM stg.transactions
            ORDER BY transaction_dt DESC
            LIMIT 1
            """
        )

        if not result.result_rows:
            return None

        return result.first_row[0]


    def get_latest_currency_date(self) -> date | None:
        result = self._client.query(
            """
            SELECT toDate(date_update)
            FROM stg.currencies
            ORDER BY date_update DESC
            LIMIT 1
            """
        )

        if not result.result_rows:
            return None

        return result.first_row[0]

    def load_transactions(
        self,
        kafka_topic: str,
        kafka_partition: int,
        offset_from: int,
        offset_to: int,
    ) -> None:
        self._client.command(
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
        self._client.command(
            self._currencies_sql,
            parameters={
                "kafka_topic": kafka_topic,
                "kafka_partition": kafka_partition,
                "offset_from": offset_from,
                "offset_to": offset_to,
            },
        )

    def close(self) -> None:
        self._client.close()