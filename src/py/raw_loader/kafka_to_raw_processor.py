import logging
import os
import time

from dotenv import load_dotenv

from lib.kafka_connect import KafkaConsumer
from raw_repository import RawRepository


BATCH_SIZE = 1000
FLUSH_INTERVAL_SECONDS = 5


class KafkaToRawProcessor:
    def __init__(
        self,
        consumer: KafkaConsumer,
        repository: RawRepository,
        logger: logging.Logger,
    ) -> None:
        self._consumer = consumer
        self._repository = repository
        self._logger = logger

    def run(self) -> None:
        self._logger.info("Kafka to RAW consumer started")

        batch = []
        last_flush_time = time.monotonic()

        try:
            while True:
                event = self._consumer.consume()

                if event is not None:
                    batch.append(event)

                flush_required = (
                    len(batch) >= BATCH_SIZE
                    or (
                        batch
                        and time.monotonic() - last_flush_time
                        >= FLUSH_INTERVAL_SECONDS
                    )
                )

                if not flush_required:
                    continue

                self._repository.save_events(batch)

                # Offset подтверждаем только после успешной записи батча в ClickHouse.
                self._consumer.commit()

                self._logger.info(
                    "Batch processed. Saved events: %s",
                    len(batch),
                )

                batch.clear()
                last_flush_time = time.monotonic()

        except KeyboardInterrupt:
            self._logger.info("Stopping consumer")

            if batch:
                try:
                    self._repository.save_events(batch)
                    self._consumer.commit()

                    self._logger.info(
                        "Final batch processed. Saved events: %s",
                        len(batch),
                    )

                except Exception:
                    self._logger.exception(
                        "Failed to save final batch. "
                        "Kafka offset was not committed."
                    )

        except Exception:
            self._logger.exception(
                "Consumer stopped because of an error. "
                "Kafka offset was not committed."
            )

        finally:
            self._consumer.close()
            self._repository.close()

            self._logger.info("Consumer stopped")


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

    repository = RawRepository(
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

    processor = KafkaToRawProcessor(
        consumer=consumer,
        repository=repository,
        logger=logger,
    )

    processor.run()


if __name__ == "__main__":
    main()