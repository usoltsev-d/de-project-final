from datetime import date
from pathlib import Path

from lib.clickhouse_client import ClickHouseClient


class CdmRepository:
    def __init__(
        self,
        clickhouse: ClickHouseClient,
        sql_dir: Path,
    ) -> None:
        self._clickhouse = clickhouse

        self._global_metrics_sql = (
            sql_dir / "load_cdm_global_metrics.sql"
        ).read_text(encoding="utf-8")

        self._check_missing_usd_rates_sql = (
            sql_dir / "check_missing_usd_rates.sql"
        ).read_text(encoding="utf-8")

    def get_missing_usd_rates(
        self,
        process_date: date,
    ) -> list[int]:
        result = self._clickhouse.client.query(
            self._check_missing_usd_rates_sql,
            parameters={
                "process_date": process_date,
            },
        )

        return [
            row[0]
            for row in result.result_rows
        ]

    def drop_global_metrics_shadow_partition(
        self,
        process_date: date,
    ) -> None:
        partition_id = self._get_partition_id(process_date)

        self._clickhouse.client.command(
            """
            ALTER TABLE cdm.global_metrics_shadow
            DROP PARTITION {partition_id:UInt32}
            """,
            parameters={
                "partition_id": partition_id,
            },
        )

    def load_global_metrics_shadow_partition(
        self,
        process_date: date,
    ) -> None:
        self._clickhouse.client.command(
            self._global_metrics_sql,
            parameters={
                "process_date": process_date,
            },
        )

    def replace_global_metrics_partition(
        self,
        process_date: date,
    ) -> None:
        partition_id = self._get_partition_id(process_date)

        self._clickhouse.client.command(
            """
            ALTER TABLE cdm.global_metrics
            REPLACE PARTITION {partition_id:UInt32}
            FROM cdm.global_metrics_shadow
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