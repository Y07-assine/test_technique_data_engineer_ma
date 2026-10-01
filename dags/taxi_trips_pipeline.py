"""DAG Chicago Taxi Trips : SODA API -> raw -> bronze -> silver -> quality -> gold -> PostgreSQL.

Le DAG ne fait qu'orchestrer : toute la logique vit dans le package `taxi_pipeline`.
La période est un paramètre du run (défaut : TRIPS_START_DATE / TRIPS_END_DATE).
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.python import get_current_context

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
}


def _period() -> tuple[str, str]:
    params = get_current_context()["params"]
    return params["start_date"], params["end_date"]


@dag(
    dag_id="taxi_trips_pipeline",
    description="Chicago Taxi Trips : ingestion SODA + medallion bronze/silver/gold + publication PostgreSQL",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={
        "start_date": Param(os.getenv("TRIPS_START_DATE", "2023-01-01"), type="string", format="date"),
        "end_date": Param(os.getenv("TRIPS_END_DATE", "2023-03-31"), type="string", format="date"),
        "force_download": Param(False, type="boolean", description="Re-télécharge les jours déjà ingérés"),
    },
    tags=["taxi", "medallion", "spark"],
    doc_md=__doc__,
)
def taxi_trips_pipeline():
    @task
    def plan_days() -> list[str]:
        from taxi_pipeline.tasks import list_days

        return list_days(*_period())

    # Un task instance par jour : retries, logs et reprise au grain du jour.
    @task(max_active_tis_per_dagrun=4, retries=3)
    def download(day: str) -> dict:
        from taxi_pipeline.tasks import download_day

        return download_day(day, force=get_current_context()["params"]["force_download"])

    @task
    def bronze() -> dict:
        from taxi_pipeline import tasks

        return tasks.bronze(*_period())

    @task
    def silver() -> dict:
        from taxi_pipeline import tasks

        return tasks.silver(*_period())

    # Pas de retry : un échec qualité est déterministe, rejouer ne changerait rien.
    @task(retries=0)
    def quality_checks() -> dict:
        from taxi_pipeline import tasks

        return tasks.quality_checks(*_period())

    @task
    def gold() -> dict:
        from taxi_pipeline import tasks

        return tasks.gold()

    @task
    def publish() -> dict:
        from taxi_pipeline import tasks

        return tasks.publish()

    downloads = download.expand(day=plan_days())
    downloads >> bronze() >> silver() >> quality_checks() >> gold() >> publish()


taxi_trips_pipeline()
