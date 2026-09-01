import logging
import clickhouse_connect
from datetime import date
from pathlib import Path

from airflow.hooks.base import BaseHook

from dds_loader.dds_processor import DdsProcessor
from dds_loader.dds_repository import DdsRepository



def load_dds_transactions(
    process_date: date,
) -> None:

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    conn = BaseHook.get_connection("clickhouse_conn")
    extra = conn.extra_dejson

    client = clickhouse_connect.get_client(
        host=conn.host,
        port=conn.port,
        username=conn.login,
        password=conn.password,
        database=conn.schema or "dds",
        secure=extra.get("secure", False),
        ca_cert=extra.get("cert_path"),
    )

    sql_dir = Path("src/sql/scripts/dds")

    repository = DdsRepository(
        client=client,
        sql_dir=sql_dir,
    )

    processor = DdsProcessor(
        drop_shadow_partition=repository.drop_transactions_shadow_partition,
        load_shadow_partition=repository.load_transactions_shadow_partition,
        replace_partition=repository.replace_transactions_partition,
    )

    try:
        processor.run(process_date)
        logging.info(
            "DDS transactions processed for date=%s",
            process_date,
        )
    finally:
        repository.close()