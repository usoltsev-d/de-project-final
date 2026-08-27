import logging
import os
from pathlib import Path

from lib.clickhouse_client import ClickHouseClient
from stg_loader.stg_processor import StgProcessor
from stg_loader.stg_repository import StgRepository


def main(last_offset: int) -> int:

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    logger = logging.getLogger(__name__)

    kafka_topic = os.environ["KAFKA_TOPIC"]
    kafka_partition = 0

    clickhouse = ClickHouseClient(
        host=os.environ["CLICKHOUSE_HOST"],
        port=int(os.environ["CLICKHOUSE_PORT"]),
        user=os.environ["CLICKHOUSE_USER"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
        database=os.environ.get(
            "CLICKHOUSE_DATABASE",
            "raw",
        ),
        secure=os.environ.get(
            "CLICKHOUSE_SECURE",
            "false",
        ).lower() == "true",
        cert_path=os.environ.get("CERT_PATH"),
    )

    sql_dir = Path("src/sql/dml/stg")

    repository = StgRepository(
        clickhouse=clickhouse,
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
        return processor.run(last_offset)
    finally:
        repository.close()