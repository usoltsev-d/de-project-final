import logging
import clickhouse_connect
from datetime import date
from pathlib import Path

from airflow.hooks.base import BaseHook

from cdm_loader.cdm_processor import CdmProcessor
from cdm_loader.cdm_repository import CdmRepository

def load_cdm_global_metrics(
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


    sql_dir = Path("src/sql/scripts/cdm")

    repository = CdmRepository(
        client=client,
        sql_dir=sql_dir,
    )

    processor = CdmProcessor(
        drop_shadow_partition=repository.drop_global_metrics_shadow_partition,
        load_shadow_partition=repository.load_global_metrics_shadow_partition,
        replace_partition=repository.replace_global_metrics_partition,
    )

    try:
        processor.run(process_date)

        logging.info(
            "CDM global_metrics processed for date=%s",
            process_date,
        )
    finally:
        repository.close()