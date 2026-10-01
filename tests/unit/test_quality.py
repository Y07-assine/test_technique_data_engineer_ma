from __future__ import annotations

from datetime import date

from taxi_pipeline.config import PipelineConfig, QualityConfig
from taxi_pipeline.quality.checks import CRITICAL, OPTIONAL_COLUMNS, WARNING, evaluate
from taxi_pipeline.silver.transform import REQUIRED_COLUMNS

CONFIG = PipelineConfig(datalake_root="unused", start_date=date(2023, 1, 1), end_date=date(2023, 1, 2))


def healthy_metrics(**overrides):
    metrics = {
        "row_count": 1000,
        "distinct_trip_ids": 1000,
        "distinct_days": 2,
        "out_of_period_rows": 0,
        "end_before_start_rows": 0,
        "invalid_duration_rows": 0,
        "invalid_distance_rows": 0,
        "invalid_amount_rows": 0,
        "amount_consistency_rate": 0.99,
        **{f"nulls__{c}": 0 for c in REQUIRED_COLUMNS + OPTIONAL_COLUMNS},
    }
    metrics.update(overrides)
    return metrics


def failed(checks):
    return {c.name: c.severity for c in checks if not c.passed}


def test_healthy_data_passes_every_check():
    assert failed(evaluate(healthy_metrics(), rejected_rows=10, config=CONFIG)) == {}


def test_missing_columns_fail_critically():
    checks = evaluate({"missing_columns": ["trip_total"]}, rejected_rows=0, config=CONFIG)
    assert failed(checks) == {"required_columns_present": CRITICAL}


def test_critical_rules():
    metrics = healthy_metrics(
        row_count=1000,
        distinct_trip_ids=998,
        distinct_days=1,
        out_of_period_rows=3,
        **{"nulls__trip_total": 1},
    )
    assert failed(evaluate(metrics, rejected_rows=0, config=CONFIG)) == {
        "no_duplicate_trip_id": CRITICAL,
        "all_days_present": CRITICAL,
        "timestamps_within_period": CRITICAL,
        "not_null__trip_total": CRITICAL,
    }


def test_empty_dataset_fails():
    metrics = healthy_metrics(row_count=0, distinct_trip_ids=0, distinct_days=0)
    assert "row_count" in failed(evaluate(metrics, rejected_rows=0, config=CONFIG))


def test_reject_rate_threshold_is_configurable():
    strict = PipelineConfig(datalake_root="unused", start_date=CONFIG.start_date, end_date=CONFIG.end_date,
                            quality=QualityConfig(max_reject_rate=0.01))
    # 50 rejets pour 1050 lignes = 4.8 %
    assert "reject_rate" not in failed(evaluate(healthy_metrics(), rejected_rows=50, config=CONFIG))
    assert failed(evaluate(healthy_metrics(), rejected_rows=50, config=strict)) == {"reject_rate": CRITICAL}


def test_soft_rules_are_warnings():
    metrics = healthy_metrics(amount_consistency_rate=0.5, **{"nulls__pickup_community_area": 400})
    assert failed(evaluate(metrics, rejected_rows=0, config=CONFIG)) == {
        "amount_consistency_rate": WARNING,
        "null_rate__pickup_community_area": WARNING,
    }
