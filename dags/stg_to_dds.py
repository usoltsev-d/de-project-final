from datetime import datetime

from datetime import datetime
from airflow.decorators import dag, task
from airflow.models import Variable

from dds_loader.load_cuload_dds_currencies import load_dds_currencies
from dds_loader.load_dds_transactions   import load_dds_transactions


start_date = datetime.fromisoformat(
    Variable.get(
        "dds_start_date",
        default_var="2022-10-01",
    )
)

end_date = datetime.fromisoformat(
    Variable.get("dds_end_date")
)


@dag(
    dag_id="stg_to_dds",
    schedule="0 1 * * *",
    start_date=start_date,
    end_date=end_date,
    catchup=True,
    max_active_runs=1,
)
def stg_to_dds():

    @task
    def load_dds_transactions_task(process_date: str) -> None:
        load_dds_transactions(
            process_date=datetime.fromisoformat(process_date).date()
        )

    @task
    def load_dds_currency_rates_task(process_date: str) -> None:
        load_dds_currencies(
            process_date=datetime.fromisoformat(process_date).date()
        )

    process_date = "{{ data_interval_start | ds }}"

    load_dds_transactions_task(process_date)
    load_dds_currencies_task(process_date)


stg_to_dds()