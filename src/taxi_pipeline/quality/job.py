from __future__ import annotations

from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession

from taxi_pipeline.config import PipelineConfig
from taxi_pipeline.quality.checks import DataQualityError, QualityReport, build_report
from taxi_pipeline.silver.job import period_filter
from taxi_pipeline.utils.observability import get_logger
from taxi_pipeline.utils.storage import ObjectStore

logger = get_logger(__name__)


def run_quality_checks(spark: SparkSession, store: ObjectStore, config: PipelineConfig) -> QualityReport:
    silver = spark.read.parquet(config.silver_trips).where(period_filter(config))
    rejected_rows = _count_rejected(spark, config)

    report = build_report(silver, rejected_rows, config)
    report_uri = f"{config.quality_reports}/period={config.start_date}_{config.end_date}/report.json"
    store.write_bytes(report_uri, report.to_json().encode())

    for check in report.checks:
        (logger.info if check.passed else logger.warning)(check.render())
    logger.info("quality report written to %s", report_uri)

    if report.failed_critical:
        names = ", ".join(c.name for c in report.failed_critical)
        raise DataQualityError(
            f"{len(report.failed_critical)} critical data quality check(s) failed: {names}. "
            f"Gold is not built. See {report_uri}"
        )
    return report


def _count_rejected(spark: SparkSession, config: PipelineConfig) -> int:
    try:
        rejected = spark.read.parquet(config.silver_rejected_trips)
    except AnalysisException as exc:
        # Aucune ligne n'a jamais été rejetée : Spark n'a écrit aucun fichier de données.
        if "UNABLE_TO_INFER_SCHEMA" in str(exc) or "PATH_NOT_FOUND" in str(exc):
            return 0
        raise
    return rejected.where(period_filter(config)).count()
