import logging
import os
import clickhouse_connect

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
        host=os.environ["KAFKA_HOST"],
        port=int(os.environ["KAFKA_PORT"]),
        topic=os.environ["KAFKA_TOPIC"],
        group=os.environ["KAFKA_CONSUMER_GROUP"],
        user=os.environ.get("KAFKA_USER"),
        password=os.environ.get("KAFKA_PASSWORD"),
        cert_path=os.environ.get("CERT_PATH"),
    )

    client = clickhouse_connect.get_client(
        host=os.environ["CLICKHOUSE_HOST"],
        port=int(os.environ["CLICKHOUSE_PORT"]),
        username=os.environ["CLICKHOUSE_USER"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
        database=os.environ.get(
            "CLICKHOUSE_DATABASE",
            "raw",
        ),
        secure=os.environ.get(
            "CLICKHOUSE_SECURE",
            "false",
        ).lower() == "true",
        ca_cert=os.environ.get("CERT_PATH"),
    )

    repository = RawRepository(
        client=client,
    )

    processor = RawProcessor(
        consumer=consumer,
        repository=repository,
        logger=logger,
    )

    processor.run()


if __name__ == "__main__":
    main()