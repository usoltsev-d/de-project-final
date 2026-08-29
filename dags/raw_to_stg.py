from datetime import datetime

from airflow.decorators import dag, task
from airflow.models import Variable

from stg_loader.load_stg_currencies import load_stg_currencies
from stg_loader.load_stg_transactions import load_stg_transactions


@dag(
    dag_id="raw_to_stg",
    schedule="* * * * *",
    start_date=datetime(2022, 10, 1),
    catchup=False,
    max_active_runs=1,
)
def raw_to_stg():

    @task
    def load_stg_transactions_task() -> None:
        offset_from = int(
            Variable.get(
                "stg_transactions_last_offset",
                default_var="-1",
            )
        )

        batch_size = int(
            Variable.get(
                "stg_batch_size",
                default_var="10000",
            )
        )

        offset_to = load_stg_transactions(
            offset_from=offset_from,
            batch_size=batch_size,
        )

        Variable.set(
            "stg_transactions_last_offset",
            offset_to,
        )

    @task
    def load_stg_currencies_task() -> None:
        offset_from = int(
            Variable.get(
                "stg_currencies_last_offset",
                default_var="-1",
            )
        )

        batch_size = int(
            Variable.get(
                "stg_batch_size",
                default_var="10000",
            )
        )

        offset_to = load_stg_currencies(
            offset_from=offset_from,
            batch_size=batch_size,
        )

        Variable.set(
            "stg_currencies_last_offset",
            offset_to,
        )

    load_stg_transactions_task()
    load_stg_currencies_task()


raw_to_stg()