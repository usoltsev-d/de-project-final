import logging
import os


from lib.clickhouse_client import ClickHouseClient
from lib.kafka_connect import KafkaConsumer
from raw_loader.raw_processor import RawProcessor
from raw_loader.raw_repository import RawRepository


def main() -> None:

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    logger = logging.getLogger(__name__)

    consumer = KafkaConsumer(
        bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        topic=os.environ["KAFKA_TOPIC"],
        group=os.environ["KAFKA_CONSUMER_GROUP"],
        security_protocol=os.environ.get(
            "KAFKA_SECURITY_PROTOCOL",
            "PLAINTEXT",
        ),
        user=os.environ.get("KAFKA_USER"),
        password=os.environ.get("KAFKA_PASSWORD"),
        cert_path=os.environ.get("CERT_PATH"),
    )

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