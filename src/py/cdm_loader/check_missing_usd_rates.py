import logging
import clickhouse_connect
from datetime import date
from pathlib import Path

from airflow.hooks.base import BaseHook

from cdm_loader.cdm_repository import CdmRepository
from lib.clickhouse_client import ClickHouseClient


def check_missing_usd_rates(
    process_date: date,
) -> None:

    conn = BaseHook.get_connection("clickhouse_conn")
    extra = conn.extra_dejson

    client = clickhouse_connect.get_client(
        host=conn.host,
        port=conn.port,
        username=conn.login,
        password=conn.password,
        database=conn.schema or "cdm",
        secure=extra.get("secure", False),
        ca_cert=extra.get("cert_path"),
    )

    sql_dir = Path("src/sql/scripts/cdm")

    repository = CdmRepository(
        client=client,
        sql_dir=sql_dir,
    )

    try:
        # Проверяем, что для всех валют с успешными транзакциями доступны прямые курсы пересчёта в USD.
        missing_rates = repository.get_missing_usd_rates(
            process_date
        )

        if missing_rates:
            raise ValueError(
                f"Missing USD rates for date={process_date}: "
                f"currency_from={missing_rates}"
            )

        logging.info(
            "USD rates validation passed for date=%s",
            process_date,
        )
    finally:
        repository.close()