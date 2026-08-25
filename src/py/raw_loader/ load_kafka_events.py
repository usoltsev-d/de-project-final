import logging
import os

from dotenv import load_dotenv

from lib.clickhouse_client import ClickHouseClient
from lib.kafka_connect import KafkaConsumer
from raw_loader.raw_processor import RawProcessor
from raw_loader.raw_repository import RawRepository


def main() -> None:
    load_dotenv()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    logger = logging.getLogger(__name__)

    consumer = KafkaConsumer(
        host=os.environ["KAFKA_HOST"],
        port=int(os.environ.get("KAFKA_PORT", "9091")),
        user=os.environ["KAFKA_USER"],
        password=os.environ["KAFKA_PASSWORD"],
        topic=os.environ["KAFKA_TOPIC"],
        group=os.environ["KAFKA_CONSUMER_GROUP"],
        cert_path=os.environ["YC_CA_PATH"],
    )

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

    repository = RawRepository(
        clickhouse=clickhouse,
    )

    processor = RawProcessor(
        consumer=consumer,
        repository=repository,
        logger=logger,
    )

    processor.run()


if __name__ == "__main__":
    main()