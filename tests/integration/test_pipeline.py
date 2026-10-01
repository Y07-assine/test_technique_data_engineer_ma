"""Un échantillon réel de l'API (+ anomalies injectées) traverse raw -> bronze -> silver -> quality -> gold."""

from __future__ import annotations

from dataclasses import replace
from datetime import date

import pytest
from pyspark.sql import functions as F

from taxi_pipeline.bronze.job import MissingRawDataError, run_bronze
from taxi_pipeline.config import QualityConfig
from taxi_pipeline.gold.job import run_gold
from taxi_pipeline.gold.tables import FACT, GOLD_TABLES
from taxi_pipeline.gold.validation import totals
from taxi_pipeline.ingestion.soda import SUCCESS_MARKER, raw_day_prefix, render_marker
from taxi_pipeline.quality.checks import DataQualityError
from taxi_pipeline.quality.job import run_quality_checks
from taxi_pipeline.silver.job import run_silver
from taxi_pipeline.utils.storage import LocalStore

FIXTURE_DAY = date(2023, 1, 1)
pytestmark = pytest.mark.integration

# tests/fixtures : 40 trajets réels valides + 5 anomalies (1 doublon exact + 4 lignes invalides)
REAL_ROWS, ANOMALIES = 40, 5


@pytest.fixture
def landed_raw(pipeline_config, fixture_csv):
    """Simule l'étape download : une page + marqueur de complétude."""
    store = LocalStore()
    prefix = raw_day_prefix(pipeline_config.raw_trips, FIXTURE_DAY)
    store.write_bytes(f"{prefix}/page_00000.csv", fixture_csv)
    store.write_bytes(f"{prefix}/{SUCCESS_MARKER}", render_marker(REAL_ROWS + ANOMALIES, 1))
    return store


def test_bronze_to_gold(spark, pipeline_config, landed_raw):
    bronze = run_bronze(spark, landed_raw, pipeline_config)
    assert bronze == {"days": 1, "bronze_rows": REAL_ROWS + ANOMALIES}

    silver = run_silver(spark, pipeline_config)
    assert silver["duplicates_removed"] == 1
    assert silver["silver_rows"] == REAL_ROWS
    assert silver["rejects_by_reason"] == {
        "empty_trip": 1,
        "end_before_start": 1,
        "missing_amount": 1,
        "negative_amount": 1,
    }

    silver_df = spark.read.parquet(pipeline_config.silver_trips)
    assert silver_df.select("trip_date").distinct().first().trip_date == FIXTURE_DAY
    assert dict(silver_df.dtypes)["trip_start_ts"] == "timestamp_ntz"

    report = run_quality_checks(spark, landed_raw, pipeline_config)
    assert report.failed_critical == []
    assert landed_raw.exists(
        f"{pipeline_config.quality_reports}/period={FIXTURE_DAY}_{FIXTURE_DAY}/report.json"
    )

    gold = run_gold(spark, pipeline_config)
    assert gold["silver_rows_used"] == REAL_ROWS
    written = {t.name: spark.read.parquet(pipeline_config.gold(t.name)) for t in GOLD_TABLES}
    for table in GOLD_TABLES:
        assert gold[f"{table.name}_rows"] == written[table.name].count() > 0
        assert written[table.name].select(*table.primary_key).distinct().count() == gold[f"{table.name}_rows"]

    silver_revenue = silver_df.agg(F.sum("trip_total")).first()[0]
    for fact in (t.name for t in GOLD_TABLES if t.kind == FACT):
        assert totals(written[fact]) == (REAL_ROWS, silver_revenue)

    assert written["dim_date"].count() == 1
    assert written["dim_zone"].count() == 78
    daily = written["daily_metrics"]
    assert daily.count() == 1
    assert daily.first().nb_trips == REAL_ROWS
    zones = written["pickup_zone_metrics"]
    assert zones.agg(F.sum("nb_trips")).first()[0] == REAL_ROWS
    assert zones.where("rank = 1").count() == 1


def test_silver_rerun_is_idempotent(spark, pipeline_config, landed_raw):
    run_bronze(spark, landed_raw, pipeline_config)
    first = run_silver(spark, pipeline_config)
    run_bronze(spark, landed_raw, pipeline_config)
    second = run_silver(spark, pipeline_config)

    assert first == second
    assert spark.read.parquet(pipeline_config.silver_trips).count() == first["silver_rows"]


def test_quality_failure_stops_the_pipeline(spark, pipeline_config, landed_raw):
    run_bronze(spark, landed_raw, pipeline_config)
    run_silver(spark, pipeline_config)
    strict = replace(pipeline_config, quality=QualityConfig(max_reject_rate=0.01))

    with pytest.raises(DataQualityError, match="reject_rate"):
        run_quality_checks(spark, landed_raw, strict)


def test_bronze_refuses_incomplete_download(spark, pipeline_config):
    with pytest.raises(MissingRawDataError, match="not ingested"):
        run_bronze(spark, LocalStore(), pipeline_config)
