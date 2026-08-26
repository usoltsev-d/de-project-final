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
            sql_dir / "load_fct_transactions_shadow.sql"
        ).read_text(encoding="utf-8")

        self._currency_rates_sql = (
            sql_dir / "load_fct_currency_rates_shadow.sql"
        ).read_text(encoding="utf-8")

    def drop_transactions_shadow_partition(
        self,
        process_date: date,
    ) -> None:
        partition_id = self._get_partition_id(process_date)

        self._clickhouse.client.command(
            """
            ALTER TABLE dds.fct_transactions_shadow
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
            ALTER TABLE dds.fct_transactions
            REPLACE PARTITION {partition_id:UInt32}
            FROM dds.fct_transactions_shadow
            """,
            parameters={
                "partition_id": partition_id,
            },
        )

    def drop_currency_rates_shadow_partition(
        self,
        process_date: date,
    ) -> None:
        partition_id = self._get_partition_id(process_date)

        self._clickhouse.client.command(
            """
            ALTER TABLE dds.fct_currency_rates_shadow
            DROP PARTITION {partition_id:UInt32}
            """,
            parameters={
                "partition_id": partition_id,
            },
        )

    def load_currency_rates_shadow_partition(
        self,
        process_date: date,
    ) -> None:
        self._clickhouse.client.command(
            self._currency_rates_sql,
            parameters={
                "process_date": process_date,
            },
        )

    def replace_currency_rates_partition(
        self,
        process_date: date,
    ) -> None:
        partition_id = self._get_partition_id(process_date)

        self._clickhouse.client.command(
            """
            ALTER TABLE dds.fct_currency_rates
            REPLACE PARTITION {partition_id:UInt32}
            FROM dds.fct_currency_rates_shadow
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

    def close(self) -> None:
        self._clickhouse.close()