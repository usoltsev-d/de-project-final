import logging
from datetime import date
from pathlib import Path

from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowSkipException

from dds_loader.dds_repository import DdsRepository
from lib.clickhouse_client import ClickHouseClient


def check_transactions_exist(
    process_date: date,
) -> None:

    conn = BaseHook.get_connection("clickhouse_conn")
    extra = conn.extra_dejson

    clickhouse = ClickHouseClient(
        host=conn.host,
        port=conn.port,
        user=conn.login,
        password=conn.password,
        database=conn.schema or "dds",
        secure=extra.get("secure", False),
        cert_path=extra.get("cert_path"),
    )

    repository = DdsRepository(
        clickhouse=clickhouse,
        sql_dir=Path("src/sql/scripts/dds"),
    )

    try:
        if not repository.has_transactions(process_date):
            raise AirflowSkipException(
                f"No transactions for date={process_date}"
            )

        logging.info(
            "Transactions validation passed for date=%s",
            process_date,
        )

    finally:
        repository.close()