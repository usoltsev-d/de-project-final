from datetime import datetime

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.sensors.external_task import ExternalTaskSensor

from cdm_loader.check_missing_usd_rates import check_missing_usd_rates
from cdm_loader.load_cdm_global_metrics import load_cdm_global_metrics


start_date = datetime.fromisoformat(
    Variable.get(
        "cdm_start_date",
        default_var="2022-10-01",
    )
)

end_date = datetime.fromisoformat(
    Variable.get(
        "cdm_end_date",
        default_var="2022-11-01",
    )
)


@dag(
    dag_id="dds_to_cdm",
    schedule="0 1 * * *",
    start_date=start_date,
    end_date=end_date,
    catchup=True,
    max_active_runs=1,
)
def dds_to_cdm():

    wait_for_dds = ExternalTaskSensor(
        task_id="wait_for_dds",
        external_dag_id="stg_to_dds",
        allowed_states=["success"],
        skipped_states=["failed"],
        mode="reschedule",
        poke_interval=30,
        timeout=3600,
    )

    @task
    def load_global_metrics_task(
        process_date: str,
    ) -> None:
        load_cdm_global_metrics(
            process_date=datetime.fromisoformat(
                process_date
            ).date()
        )

    @task
    def check_missing_usd_rates_task(
        process_date: str,
    ) -> None:
        check_missing_usd_rates(
            process_date=datetime.fromisoformat(
                process_date
            ).date()
        )

    process_date = "{{ data_interval_start | ds }}"

    wait_for_dds >> load_global_metrics_task(process_date) >> check_missing_usd_rates_task(process_date)


dds_to_cdm()