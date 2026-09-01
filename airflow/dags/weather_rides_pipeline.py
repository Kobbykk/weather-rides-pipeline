"""Monthly pipeline: ingest taxi trips + weather, then transform with dbt."""

from datetime import datetime, timedelta

from airflow.sdk import dag
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, Param

PROJECT_DIR = "/opt/airflow/project"
MONTH = "{{ params.month or data_interval_start.strftime('%Y-%m') }}"

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="weather_rides_pipeline",
    description="Load NYC taxi trips and weather, then build dbt models.",
    schedule="@monthly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["portfolio", "elt"],
    params={
        "month": Param(
            default=None,
            type=["null", "string"],
            pattern=r"^\d{4}-\d{2}$",
            title="Month to load",
            description="Format YYYY-MM, e.g. 2024-05. Leave empty to use the run's own interval.",
        )
    },
)
def weather_rides_pipeline():

    ingest_trips = BashOperator(
        task_id="ingest_trips",
        bash_command=(
            f"python {PROJECT_DIR}/src/ingestion/load_trips.py --month {MONTH}"
        ),
    )

    ingest_weather = BashOperator(
        task_id="ingest_weather",
        bash_command=(
            f"python {PROJECT_DIR}/src/ingestion/load_weather.py --month {MONTH}"
        ),
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"cd {PROJECT_DIR}/transform && dbt build --target dev"
        ),
    )

    [ingest_trips, ingest_weather] >> dbt_build


weather_rides_pipeline()