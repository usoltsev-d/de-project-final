import logging
from collections.abc import Callable


class StgProcessor:
    def __init__(
        self,
        get_high_watermark: Callable[[], int | None],
        load_batch: Callable[[int, int], None],
        logger: logging.Logger,
    ) -> None:
        self._get_high_watermark = get_high_watermark
        self._load_batch = load_batch
        self._logger = logger

    def run(
        self,
        offset_from: int,
        batch_size: int,
    ) -> int:
        high_watermark = self._get_high_watermark()

        if high_watermark is None:
            self._logger.info("RAW table is empty")
            return offset_from

        if offset_from >= high_watermark:
            self._logger.info(
                "No new RAW events. "
                "offset_from=%s, high_watermark=%s",
                offset_from,
                high_watermark,
            )
            return offset_from

        offset_to = min(
            offset_from + batch_size,
            high_watermark,
        )

        self._logger.info(
            "Processing RAW offsets (%s, %s]",
            offset_from,
            offset_to,
        )

        self._load_batch(
            offset_from,
            offset_to,
        )

        self._logger.info(
            "Batch processed successfully. "
            "offset_to=%s",
            offset_to,
        )

        return offset_to