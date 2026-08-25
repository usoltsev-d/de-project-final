import logging
from collections.abc import Callable

from stg_loader.checkpoint import FileCheckpoint


BATCH_SIZE = 10000


class StgProcessor:
    def __init__(
        self,
        checkpoint: FileCheckpoint,
        get_high_watermark: Callable[[], int | None],
        load_batch: Callable[[int, int], None],
        logger: logging.Logger,
    ) -> None:
        self._checkpoint = checkpoint
        self._get_high_watermark = get_high_watermark
        self._load_batch = load_batch
        self._logger = logger

    def run(self) -> None:
        last_offset = self._checkpoint.get()
        high_watermark = self._get_high_watermark()

        if high_watermark is None:
            self._logger.info("RAW table is empty")
            return

        if last_offset >= high_watermark:
            self._logger.info(
                "No new RAW events. "
                "last_offset=%s, high_watermark=%s",
                last_offset,
                high_watermark,
            )
            return

        self._logger.info(
            "RAW to STG processing started. "
            "last_offset=%s, high_watermark=%s",
            last_offset,
            high_watermark,
        )

        while last_offset < high_watermark:
            offset_to = min(
                last_offset + BATCH_SIZE,
                high_watermark,
            )

            self._logger.info(
                "Processing RAW offsets (%s, %s]",
                last_offset,
                offset_to,
            )

            self._load_batch(
                last_offset,
                offset_to,
            )

            # Checkpoint двигаем только после успешной записи batch в STG.
            self._checkpoint.set(offset_to)

            last_offset = offset_to

            self._logger.info(
                "Batch processed successfully. "
                "Checkpoint=%s",
                last_offset,
            )

        self._logger.info(
            "RAW to STG processing completed. "
            "Checkpoint=%s",
            last_offset,
        )