import logging
import clickhouse_connect
from datetime import date
from pathlib import Path

from airflow.hooks.base import BaseHook

from dds_loader.dds_processor import DdsProcessor
from dds_loader.dds_repository import DdsRepository


def load_dds_currencies(
    process_date: date,
) -> None:

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

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

    processor = DdsProcessor(
        drop_shadow_partition=repository.drop_currencies_shadow_partition,
        load_shadow_partition=repository.load_currencies_shadow_partition,
        replace_partition=repository.replace_currencies_partition,
    )

    try:
        processor.run(process_date)
        logging.info(
            "DDS currencies processed for date=%s",
            process_date,
        )
    finally:
        repository.close()