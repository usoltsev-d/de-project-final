import logging
from collections.abc import Callable


BATCH_SIZE = 10000


class StgProcessor:
    def __init__(
        self,
        get_high_watermark: Callable[[], int | None],
        load_batch: Callable[[int, int], None],
        save_offset: Callable[[int], None],
        logger: logging.Logger,
    ) -> None:
        self._get_high_watermark = get_high_watermark
        self._load_batch = load_batch
        self._save_offset = save_offset
        self._logger = logger

    def run(
        self,
        last_offset: int,
    ) -> int:
        high_watermark = self._get_high_watermark()

        if high_watermark is None:
            self._logger.info("RAW table is empty")
            return last_offset

        if last_offset >= high_watermark:
            self._logger.info(
                "No new RAW events. "
                "last_offset=%s, high_watermark=%s",
                last_offset,
                high_watermark,
            )
            return last_offset

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

            # Сохраняем offset только после успешной загрузки batch.
            self._save_offset(offset_to)

            last_offset = offset_to

            self._logger.info(
                "Batch processed successfully. "
                "last_offset=%s",
                last_offset,
            )

        self._logger.info(
            "RAW to STG processing completed. "
            "last_offset=%s",
            last_offset,
        )

        return last_offset