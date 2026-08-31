from datetime import datetime
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.sensors.python import PythonSensor

from dds_loader.load_dds_currencies import load_dds_currencies
from dds_loader.load_dds_transactions import load_dds_transactions
from dds_loader.check_transactions_exist import check_transactions_exist
from dds_loader.check_currencies_exist import check_currencies_exist
from dds_loader.check_stg_ready import is_stg_date_ready


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

    process_date = "{{ data_interval_start | ds }}"

    wait_for_stg_date = PythonSensor(
        task_id="wait_for_stg_date",
        python_callable=is_stg_date_ready,
        op_kwargs={
            "process_date": process_date,
        },
        mode="reschedule",
        poke_interval=300,
        timeout=21600,
    )

    @task
    def load_dds_transactions_task(
        process_date: str,
    ) -> None:
        load_dds_transactions(
            process_date=datetime.fromisoformat(
                process_date
            ).date()
        )

    @task
    def load_dds_currencies_task(
        process_date: str,
    ) -> None:
        load_dds_currencies(
            process_date=datetime.fromisoformat(
                process_date
            ).date()
        )

    @task
    def check_transactions_exist_task(
        process_date: str,
    ) -> None:
        check_transactions_exist(
            process_date=datetime.fromisoformat(
                process_date
            ).date()
        )

    @task
    def check_currencies_exist_task(
        process_date: str,
    ) -> None:
        check_currencies_exist(
            process_date=datetime.fromisoformat(
                process_date
            ).date()
    )

    transactions = load_dds_transactions_task(
        process_date
    )

    currencies = load_dds_currencies_task(
        process_date
    )

    transactions_check = check_transactions_exist_task(
        process_date
    )

    currencies_check = check_currencies_exist_task(
        process_date
    )

    wait_for_stg_date >> [transactions, currencies]

    transactions >> transactions_check
    currencies >> currencies_check

stg_to_dds()