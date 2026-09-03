import logging
import clickhouse_connect
from datetime import date
from pathlib import Path

from airflow.hooks.base import BaseHook

from dds_loader.dds_repository import DdsRepository


def check_transactions_exist(
    process_date: date,
) -> None:

    conn = BaseHook.get_connection("clickhouse_conn")

    client = clickhouse_connect.get_client(
        host=conn.host,
        port=conn.port,
        username=conn.login,
        password=conn.password,
    )

    sql_dir = Path("src/sql/scripts/dds")

    repository = DdsRepository(
        client=client,
        sql_dir=sql_dir,
    )

    try:
        if not repository.has_transactions(process_date):
            raise ValueError(
                f"No transactions for date={process_date}"
            )

        logging.info(
            "Transactions validation passed for date=%s",
            process_date,
        )

    finally:
        repository.close()