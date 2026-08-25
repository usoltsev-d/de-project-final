import logging
import os
from pathlib import Path

from dotenv import load_dotenv

from lib.clickhouse_client import ClickHouseClient
from stg_loader.checkpoint import FileCheckpoint
from stg_loader.raw_to_stg_processor import RawToStgProcessor
from stg_loader.stg_repository import StgRepository

CHECKPOINT_DIR = Path(__file__).resolve().parent / "checkpoints"

def main() -> None:
    load_dotenv()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    logger = logging.getLogger(__name__)

    kafka_topic = os.environ["KAFKA_TOPIC"]
    kafka_partition = 0

    clickhouse = ClickHouseClient(
        host=os.environ["CLICKHOUSE_HOST"],
        port=int(os.environ.get("CLICKHOUSE_PORT", "8443")),
        user=os.environ["CLICKHOUSE_USER"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
        database=os.environ.get(
            "CLICKHOUSE_DATABASE",
            "raw",
        ),
        cert_path=os.environ["YC_CA_PATH"],
    )

    sql_dir = Path("src/sql/dml/stg")

    repository = StgRepository(
        clickhouse=clickhouse,
        sql_dir=sql_dir,
    )

    checkpoint = FileCheckpoint(
        CHECKPOINT_DIR / "stg_currencies.offset"
    )

    processor = RawToStgProcessor(
        checkpoint=checkpoint,
        get_high_watermark=lambda: repository.get_max_raw_offset(
            kafka_topic=kafka_topic,
            kafka_partition=kafka_partition,
        ),
        load_batch=lambda offset_from, offset_to: repository.load_currencies(
            kafka_topic=kafka_topic,
            kafka_partition=kafka_partition,
            offset_from=offset_from,
            offset_to=offset_to,
        ),
        logger=logger,
    )

    try:
        processor.run()
    finally:
        repository.close()


if __name__ == "__main__":
    main()