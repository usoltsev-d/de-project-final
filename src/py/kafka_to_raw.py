import json
import logging
import os
import time
from datetime import datetime
from uuid import UUID

from dotenv import load_dotenv

from kafka_connect import KafkaConsumer
from raw_repository import RawRepository


BATCH_SIZE = 500
FLUSH_INTERVAL_SECONDS = 5

ALLOWED_OBJECT_TYPES = {
    "TRANSACTION",
    "CURRENCY",
}


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

    def _parse_event(self, message: dict) -> tuple | None:
        object_type = message.get("object_type")

        if object_type not in ALLOWED_OBJECT_TYPES:
            self._logger.info(
                "Service or unknown message skipped: %s",
                message,
            )
            return None

        try:
            return (
                UUID(message["object_id"]),
                object_type,
                datetime.fromisoformat(message["sent_dttm"]),
                json.dumps(
                    message["payload"],
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
            )

        except (KeyError, TypeError, ValueError) as exc:
            self._logger.warning(
                "Invalid event skipped: %s. Error: %s",
                message,
                exc,
            )
            return None

    def run(self) -> None:
        self._logger.info("Kafka to RAW consumer started")

        batch = []
        has_consumed_messages = False
        last_flush_time = time.monotonic()

        try:
            while True:
                message = self._consumer.consume()

                if message is not None:
                    has_consumed_messages = True

                    event = self._parse_event(message)

                    if event is not None:
                        batch.append(event)

                flush_required = (
                    len(batch) >= BATCH_SIZE
                    or (
                        has_consumed_messages
                        and time.monotonic() - last_flush_time
                        >= FLUSH_INTERVAL_SECONDS
                    )
                )

                if not flush_required:
                    continue

                self._repository.save_events(batch)

                # Commit делаем только после успешного INSERT.
                self._consumer.commit()

                self._logger.info(
                    "Batch processed. Saved business events: %s",
                    len(batch),
                )

                batch.clear()
                has_consumed_messages = False
                last_flush_time = time.monotonic()

        except KeyboardInterrupt:
            self._logger.info("Stopping consumer")

            if has_consumed_messages:
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
            # Если запись в ClickHouse упала, commit не выполняется.
            # После перезапуска Kafka отдаст события повторно.
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