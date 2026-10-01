"""Points d'entrée des étapes du pipeline, appelés par le DAG Airflow et les scripts.

Chaque étape : construit sa configuration, ouvre/ferme sa propre SparkSession,
mesure sa durée et renvoie des métriques sérialisables (XCom, logs).
"""

from __future__ import annotations

from datetime import date

from taxi_pipeline.bronze.job import run_bronze
from taxi_pipeline.config import PipelineConfig, load_config
from taxi_pipeline.gold.job import run_gold
from taxi_pipeline.ingestion.soda import SodaClient, daterange, ingest_day
from taxi_pipeline.publish.postgres import run_publish
from taxi_pipeline.quality.job import run_quality_checks
from taxi_pipeline.silver.job import run_silver
from taxi_pipeline.utils.observability import get_logger, timed_step
from taxi_pipeline.utils.spark import spark_session
from taxi_pipeline.utils.storage import get_store

logger = get_logger("taxi_pipeline")


def resolve_config(start: str | None = None, end: str | None = None) -> PipelineConfig:
    """Configuration d'environnement, avec surcharge éventuelle de la période."""
    config = load_config()
    return config.with_period(
        date.fromisoformat(start) if start else config.start_date,
        date.fromisoformat(end) if end else config.end_date,
    )


def list_days(start: str | None = None, end: str | None = None) -> list[str]:
    config = resolve_config(start, end)
    return [d.isoformat() for d in daterange(config.start_date, config.end_date)]


def download_day(day: str, force: bool = False) -> dict:
    config = load_config()
    with timed_step(logger, f"download[{day}]", {}) as metrics:
        result = ingest_day(
            date.fromisoformat(day),
            SodaClient(config.soda),
            get_store(config.datalake_root, config.s3),
            config.raw_trips,
            force=force,
        )
        metrics.update(rows=result.rows, pages=result.pages, skipped=result.skipped)
    return metrics


def bronze(start: str | None = None, end: str | None = None) -> dict:
    config = resolve_config(start, end)
    with timed_step(logger, "bronze", {}) as metrics, spark_session(config, "taxi-bronze") as spark:
        metrics.update(run_bronze(spark, get_store(config.datalake_root, config.s3), config))
    return metrics


def silver(start: str | None = None, end: str | None = None) -> dict:
    config = resolve_config(start, end)
    with timed_step(logger, "silver", {}) as metrics, spark_session(config, "taxi-silver") as spark:
        metrics.update(run_silver(spark, config))
    return metrics


def quality_checks(start: str | None = None, end: str | None = None) -> dict:
    config = resolve_config(start, end)
    with timed_step(logger, "quality_checks", {}) as metrics, spark_session(config, "taxi-quality") as spark:
        report = run_quality_checks(spark, get_store(config.datalake_root, config.s3), config)
        metrics.update(
            checks=len(report.checks),
            failed_warning_checks=[c.name for c in report.checks if not c.passed],
            row_count=report.metrics["row_count"],
        )
    return metrics


def gold() -> dict:
    config = load_config()
    with timed_step(logger, "gold", {}) as metrics, spark_session(config, "taxi-gold") as spark:
        metrics.update(run_gold(spark, config))
    return metrics


def publish() -> dict:
    config = load_config()
    with timed_step(logger, "publish", {}) as metrics, spark_session(config, "taxi-publish") as spark:
        metrics.update(run_publish(spark, config))
    return metrics
