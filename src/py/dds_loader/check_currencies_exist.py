import logging
from datetime import date
from pathlib import Path

from airflow.hooks.base import BaseHook

from dds_loader.dds_repository import DdsRepository
from lib.clickhouse_client import ClickHouseClient


def check_currencies_exist(
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
        if not repository.has_currencies(process_date):
            raise ValueError(
                f"No currency rates for date={process_date}"
            )

        logging.info(
            "Currencies validation passed for date=%s",
            process_date,
        )

    finally:
        repository.close()