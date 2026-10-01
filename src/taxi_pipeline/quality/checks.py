"""Contrôles qualité de la couche SILVER.

Les métriques sont calculées en une seule agrégation Spark (un seul scan de la
table), puis évaluées contre des seuils. Un contrôle CRITICAL en échec fait
échouer le pipeline (DataQualityError) : la couche GOLD n'est jamais construite
sur une donnée jugée non fiable. Un contrôle WARNING est seulement signalé.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from taxi_pipeline.config import PipelineConfig
from taxi_pipeline.silver.transform import MAX_TRIP_MILES, MAX_TRIP_SECONDS, MAX_TRIP_TOTAL, REQUIRED_COLUMNS

CRITICAL = "CRITICAL"
WARNING = "WARNING"

OPTIONAL_COLUMNS = ["pickup_community_area", "dropoff_community_area", "taxi_id", "payment_type", "company"]


class DataQualityError(RuntimeError):
    pass


@dataclass(frozen=True)
class CheckResult:
    name: str
    severity: str
    passed: bool
    observed: Any
    expected: str

    def render(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        return f"[{status}] {self.severity:<8} {self.name}: observed={self.observed} expected {self.expected}"


@dataclass(frozen=True)
class QualityReport:
    period: str
    generated_at: str
    metrics: dict[str, Any]
    checks: list[CheckResult]

    @property
    def failed_critical(self) -> list[CheckResult]:
        return [c for c in self.checks if not c.passed and c.severity == CRITICAL]

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2, default=str)


def compute_metrics(silver: DataFrame, config: PipelineConfig) -> dict[str, Any]:
    """Toutes les métriques de la table silver en une passe."""
    missing = [c for c in REQUIRED_COLUMNS + OPTIONAL_COLUMNS if c not in silver.columns]
    if missing:
        return {"missing_columns": missing}

    def nulls(col: str):
        return F.sum(F.col(col).isNull().cast("int")).alias(f"nulls__{col}")

    row = silver.agg(
        F.count("*").alias("row_count"),
        F.countDistinct("trip_id").alias("distinct_trip_ids"),
        F.countDistinct("trip_date").alias("distinct_days"),
        *[nulls(c) for c in REQUIRED_COLUMNS + OPTIONAL_COLUMNS],
        F.sum((~F.col("trip_date").between(F.lit(config.start_date), F.lit(config.end_date))).cast("int"))
        .alias("out_of_period_rows"),
        F.sum((F.col("trip_end_ts") < F.col("trip_start_ts")).cast("int")).alias("end_before_start_rows"),
        F.sum((~F.col("trip_seconds").between(0, MAX_TRIP_SECONDS)).cast("int")).alias("invalid_duration_rows"),
        F.sum((~F.col("trip_miles").between(0, MAX_TRIP_MILES)).cast("int")).alias("invalid_distance_rows"),
        F.sum(((F.col("fare") < 0) | (F.col("trip_total") < 0) | (F.col("trip_total") > MAX_TRIP_TOTAL)).cast("int"))
        .alias("invalid_amount_rows"),
        F.avg(F.col("is_amount_consistent").cast("double")).alias("amount_consistency_rate"),
    ).first().asDict()
    return {k: (v if v is not None else 0) for k, v in row.items()}


def evaluate(metrics: dict[str, Any], rejected_rows: int, config: PipelineConfig) -> list[CheckResult]:
    if "missing_columns" in metrics:
        return [CheckResult("required_columns_present", CRITICAL, False, metrics["missing_columns"], "no missing column")]

    rows = metrics["row_count"]
    q = config.quality
    expected_days = (config.end_date - config.start_date).days + 1
    reject_rate = rejected_rows / (rows + rejected_rows) if rows + rejected_rows else 0.0

    checks = [
        CheckResult("required_columns_present", CRITICAL, True, [], "no missing column"),
        CheckResult("row_count", CRITICAL, rows > 0, rows, "> 0"),
        CheckResult("all_days_present", CRITICAL, metrics["distinct_days"] == expected_days,
                    metrics["distinct_days"], f"== {expected_days}"),
        CheckResult("no_duplicate_trip_id", CRITICAL, metrics["distinct_trip_ids"] == rows,
                    rows - metrics["distinct_trip_ids"], "== 0 duplicates"),
        CheckResult("timestamps_within_period", CRITICAL, metrics["out_of_period_rows"] == 0,
                    metrics["out_of_period_rows"], "== 0"),
        CheckResult("end_after_start", CRITICAL, metrics["end_before_start_rows"] == 0,
                    metrics["end_before_start_rows"], "== 0"),
        CheckResult("duration_valid", CRITICAL, metrics["invalid_duration_rows"] == 0,
                    metrics["invalid_duration_rows"], "== 0"),
        CheckResult("distance_valid", CRITICAL, metrics["invalid_distance_rows"] == 0,
                    metrics["invalid_distance_rows"], "== 0"),
        CheckResult("amounts_valid", CRITICAL, metrics["invalid_amount_rows"] == 0,
                    metrics["invalid_amount_rows"], "== 0"),
        CheckResult("reject_rate", CRITICAL, reject_rate <= q.max_reject_rate,
                    round(reject_rate, 4), f"<= {q.max_reject_rate}"),
        CheckResult("amount_consistency_rate", WARNING,
                    metrics["amount_consistency_rate"] >= q.min_amount_consistency_rate,
                    round(metrics["amount_consistency_rate"], 4), f">= {q.min_amount_consistency_rate}"),
    ]
    for col in REQUIRED_COLUMNS:
        checks.append(CheckResult(f"not_null__{col}", CRITICAL, metrics[f"nulls__{col}"] == 0,
                                  metrics[f"nulls__{col}"], "== 0"))
    for col in OPTIONAL_COLUMNS:
        rate = metrics[f"nulls__{col}"] / rows if rows else 0.0
        checks.append(CheckResult(f"null_rate__{col}", WARNING, rate <= q.max_optional_null_rate,
                                  round(rate, 4), f"<= {q.max_optional_null_rate}"))
    return checks


def build_report(silver: DataFrame, rejected_rows: int, config: PipelineConfig) -> QualityReport:
    metrics = compute_metrics(silver, config)
    return QualityReport(
        period=f"{config.start_date}..{config.end_date}",
        generated_at=datetime.now(timezone.utc).isoformat(),
        metrics={**metrics, "rejected_rows": rejected_rows},
        checks=evaluate(metrics, rejected_rows, config),
    )
