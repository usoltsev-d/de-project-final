import logging
import clickhouse_connect
from datetime import datetime
from pathlib import Path

from airflow.hooks.base import BaseHook

from stg_loader.stg_repository import StgRepository


def is_stg_date_ready(
    process_date: str,
) -> bool:
    process_date_value = datetime.fromisoformat(
        process_date
    ).date()

    conn = BaseHook.get_connection("clickhouse_conn")

    client = clickhouse_connect.get_client(
        host=conn.host,
        port=conn.port,
        username=conn.login,
        password=conn.password,
    )

    sql_dir = Path("src/sql/scripts/stg")

    repository = StgRepository(
        client=client,
        sql_dir=sql_dir,
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
            and latest_currency_date > process_date_value
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