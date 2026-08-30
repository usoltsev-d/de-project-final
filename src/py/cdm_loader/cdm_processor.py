from collections.abc import Callable
from datetime import date


class CdmProcessor:
    def __init__(
        self,
        get_missing_usd_rates: Callable[[date], list[int]],
        drop_shadow_partition: Callable[[date], None],
        load_shadow_partition: Callable[[date], None],
        replace_partition: Callable[[date], None],
    ) -> None:
        self._get_missing_usd_rates = get_missing_usd_rates
        self._drop_shadow_partition = drop_shadow_partition
        self._load_shadow_partition = load_shadow_partition
        self._replace_partition = replace_partition

    def run(
        self,
        process_date: date,
    ) -> None:
        missing_rates = self._get_missing_usd_rates(process_date)

        if missing_rates:
            raise ValueError(
                f"Missing USD rates for date={process_date}: "
                f"currency_from={missing_rates}"
            )

        self._drop_shadow_partition(process_date)
        self._load_shadow_partition(process_date)
        self._replace_partition(process_date)