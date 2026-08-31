from datetime import date
from pathlib import Path

from lib.clickhouse_client import ClickHouseClient


class DdsRepository:
    def __init__(
        self,
        clickhouse: ClickHouseClient,
        sql_dir: Path,
    ) -> None:
        self._clickhouse = clickhouse

        self._transactions_sql = (
            sql_dir / "load_dds_transactions.sql"
        ).read_text(encoding="utf-8")

        self._currencies_sql = (
            sql_dir / "load_dds_currencies.sql"
        ).read_text(encoding="utf-8")

    def drop_transactions_shadow_partition(
        self,
        process_date: date,
    ) -> None:
        partition_id = self._get_partition_id(process_date)

        self._clickhouse.client.command(
            """
            ALTER TABLE dds.transactions_shadow
            DROP PARTITION {partition_id:UInt32}
            """,
            parameters={
                "partition_id": partition_id,
            },
        )

    def load_transactions_shadow_partition(
        self,
        process_date: date,
    ) -> None:
        self._clickhouse.client.command(
            self._transactions_sql,
            parameters={
                "process_date": process_date,
            },
        )

    def replace_transactions_partition(
        self,
        process_date: date,
    ) -> None:
        partition_id = self._get_partition_id(process_date)

        self._clickhouse.client.command(
            """
            ALTER TABLE dds.transactions
            REPLACE PARTITION {partition_id:UInt32}
            FROM dds.transactions_shadow
            """,
            parameters={
                "partition_id": partition_id,
            },
        )

    def drop_currencies_shadow_partition(
        self,
        process_date: date,
    ) -> None:
        partition_id = self._get_partition_id(process_date)

        self._clickhouse.client.command(
            """
            ALTER TABLE dds.currencies_shadow
            DROP PARTITION {partition_id:UInt32}
            """,
            parameters={
                "partition_id": partition_id,
            },
        )

    def load_currencies_shadow_partition(
        self,
        process_date: date,
    ) -> None:
        self._clickhouse.client.command(
            self._currencies_sql,
            parameters={
                "process_date": process_date,
            },
        )

    def replace_currencies_partition(
        self,
        process_date: date,
    ) -> None:
        partition_id = self._get_partition_id(process_date)

        self._clickhouse.client.command(
            """
            ALTER TABLE dds.currencies
            REPLACE PARTITION {partition_id:UInt32}
            FROM dds.currencies_shadow
            """,
            parameters={
                "partition_id": partition_id,
            },
        )

    def _get_partition_id(
        self,
        process_date: date,
    ) -> int:
        return int(process_date.strftime("%Y%m%d"))

    def has_transactions(
        self,
        process_date: date,
    ) -> bool:
        result = self._clickhouse.client.query(
            """
            SELECT 1
            FROM dds.transactions
            WHERE transaction_dt >= toDateTime64(
                {process_date:Date},
                3,
                'UTC'
            )
            AND transaction_dt < toDateTime64(
                {process_date:Date} + INTERVAL 1 DAY,
                3,
                'UTC'
            )
            LIMIT 1
            """,
            parameters={
                "process_date": process_date,
            },
        )

        return bool(result.result_rows)

    def has_currencies(
        self,
        process_date: date,
    ) -> bool:
        result = self._clickhouse.client.query(
            """
            SELECT 1
            FROM dds.currencies
            WHERE rate_date = {process_date:Date}
            LIMIT 1
            """,
            parameters={
                "process_date": process_date,
            },
        )

        return bool(result.result_rows)

    def close(self) -> None:
        self._clickhouse.close()