import logging
import time

from lib.kafka_connect import KafkaConsumer
from raw_loader.raw_repository import RawRepository


BATCH_SIZE = 5000
FLUSH_INTERVAL_SECONDS = 30


class RawProcessor:
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

                self._flush(batch)
                batch.clear()
                last_flush_time = time.monotonic()

        except KeyboardInterrupt:
            self._logger.info("Stopping consumer")

            if batch:
                try:
                    self._flush(batch)
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

    def _flush(self, batch: list) -> None:
        saved_events = self._repository.save_events(batch)

        self._consumer.commit()

        self._logger.info(
            "Batch processed. Received events: %s, "
            "saved events: %s, "
            "skipped duplicates: %s",
            len(batch),
            saved_events,
            len(batch) - saved_events,
        )