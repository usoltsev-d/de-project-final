import logging
import os
from pathlib import Path

import clickhouse_connect
from stg_loader.stg_processor import StgProcessor
from stg_loader.stg_repository import StgRepository
from airflow.hooks.base import BaseHook


def load_stg_transactions(
    offset_from: int,
    batch_size: int,
) -> int:

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    logger = logging.getLogger(__name__)

    kafka_topic = os.environ["KAFKA_TOPIC"]
    kafka_partition = 0

    conn = BaseHook.get_connection("clickhouse_conn")
    extra = conn.extra_dejson

    client = clickhouse_connect.get_client(
        host=conn.host,
        port=conn.port,
        user=conn.login,
        password=conn.password,
        database=conn.schema or "raw",
        secure=extra.get("secure", False),
        cert_path=extra.get("cert_path"),
    )

    sql_dir = Path("src/sql/scripts/stg")

    repository = StgRepository(
        client=client,
        sql_dir=sql_dir,
    )

    processor = StgProcessor(
        get_high_watermark=lambda: repository.get_max_raw_offset(
            kafka_topic=kafka_topic,
            kafka_partition=kafka_partition,
        ),
        load_batch=lambda offset_from, offset_to: repository.load_transactions(
            kafka_topic=kafka_topic,
            kafka_partition=kafka_partition,
            offset_from=offset_from,
            offset_to=offset_to,
        ),
        logger=logger,
    )

    try:
        return processor.run(
            offset_from=offset_from,
            batch_size=batch_size,
        )
    finally:
        repository.close()