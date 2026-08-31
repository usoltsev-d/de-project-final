import logging
from datetime import datetime
from pathlib import Path

from airflow.hooks.base import BaseHook

from lib.clickhouse_client import ClickHouseClient
from stg_loader.stg_repository import StgRepository


def is_stg_date_ready(
    process_date: str,
) -> bool:
    process_date_value = datetime.fromisoformat(
        process_date
    ).date()

    conn = BaseHook.get_connection("clickhouse_conn")
    extra = conn.extra_dejson

    clickhouse = ClickHouseClient(
        host=conn.host,
        port=conn.port,
        user=conn.login,
        password=conn.password,
        database=conn.schema or "raw",
        secure=extra.get("secure", False),
        cert_path=extra.get("cert_path"),
    )

    repository = StgRepository(
        clickhouse=clickhouse,
        sql_dir=Path("src/sql/scripts/stg"),
    )

    try:
        latest_transaction_date = (
            repository.get_latest_transaction_date()
        )

        latest_currency_date = (
            repository.get_latest_currency_date()
        )

        if (
            latest_transaction_date is None
            or latest_currency_date is None
        ):
            logging.info(
                "STG is not ready for date=%s: "
                "transaction_date=%s, currency_date=%s",
                process_date_value,
                latest_transaction_date,
                latest_currency_date,
            )
            return False

        ready = (
            latest_transaction_date > process_date_value
            and latest_currency_date >= process_date_value
        )

        logging.info(
            "STG ready for date=%s: "
            "transaction_date=%s, currency_date=%s, ready=%s",
            process_date_value,
            latest_transaction_date,
            latest_currency_date,
            ready,
        )

        return ready

    finally:
        repository.close()