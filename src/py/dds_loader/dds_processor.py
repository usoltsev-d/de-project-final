from collections.abc import Callable
from datetime import date


class DdsProcessor:
    def __init__(
        self,
        drop_shadow_partition: Callable[[date], None],
        load_shadow_partition: Callable[[date], None],
        replace_partition: Callable[[date], None],
    ) -> None:
        self._drop_shadow_partition = drop_shadow_partition
        self._load_shadow_partition = load_shadow_partition
        self._replace_partition = replace_partition

    def run(
        self,
        process_date: date,
    ) -> None:
        self._drop_shadow_partition(process_date)
        self._load_shadow_partition(process_date)
        self._replace_partition(process_date)