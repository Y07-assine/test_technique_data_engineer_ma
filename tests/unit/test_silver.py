from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from taxi_pipeline.bronze.job import SOURCE_COLUMNS, SOURCE_SCHEMA
from taxi_pipeline.silver.transform import cast_columns, deduplicate, flag, split_valid_rejected

SOURCE_FILE = "s3a://datalake/raw/taxi_trips/trip_date=2023-01-01/page_00000.csv"


def source_row(trip_id, **overrides):
    row = dict.fromkeys(SOURCE_COLUMNS)
    row.update(
        trip_id=trip_id,
        taxi_id="taxi-1",
        trip_start_timestamp="2023-01-01T08:00:00.000",
        trip_end_timestamp="2023-01-01T08:15:00.000",
        trip_seconds="900",
        trip_miles="3.5",
        pickup_community_area="8",
        dropoff_community_area="32",
        fare="12.50",
        tips="2.50",
        tolls="0",
        extras="1",
        trip_total="16.00",
        payment_type="credit card",
        company=" Flash Cab ",
    )
    row.update(overrides)
    return row


@pytest.fixture
def to_bronze_df(spark):
    def build(rows):
        df = spark.createDataFrame([tuple(r[c] for c in SOURCE_COLUMNS) for r in rows], SOURCE_SCHEMA)
        return df.withColumn("_source_file", F.lit(SOURCE_FILE)).withColumn("trip_date", F.lit("2023-01-01"))

    return build


def process(bronze):
    return split_valid_rejected(flag(deduplicate(cast_columns(bronze))))


def test_valid_trip_is_typed_normalized_and_enriched(to_bronze_df):
    valid, rejected = process(to_bronze_df([source_row("a")]))

    assert rejected.count() == 0
    row = valid.first()
    assert row.trip_start_ts.isoformat() == "2023-01-01T08:00:00"
    assert row.trip_date == date(2023, 1, 1)
    assert (row.trip_hour, row.day_of_week) == (8, "Sunday")
    assert row.trip_minutes == 15.0
    assert row.avg_speed_mph == 14.0
    assert row.tip_pct == 20.0
    assert row.trip_total == Decimal("16.00")
    assert row.payment_type == "Credit Card"
    assert row.company == "Flash Cab"
    assert row.is_amount_consistent is True


def test_blank_strings_become_nulls(to_bronze_df):
    valid, _ = process(to_bronze_df([source_row("a", pickup_community_area="", tips=" ")]))
    row = valid.first()
    assert row.pickup_community_area is None
    assert row.tips is None


def test_duplicates_are_removed(to_bronze_df):
    valid, _ = process(to_bronze_df([source_row("a"), source_row("a"), source_row("b")]))
    assert sorted(r.trip_id for r in valid.collect()) == ["a", "b"]


def test_amount_inconsistency_is_flagged_not_rejected(to_bronze_df):
    valid, rejected = process(to_bronze_df([source_row("a", trip_total="99.00")]))
    assert rejected.count() == 0
    row = valid.first()
    assert row.amount_gap == Decimal("83.00")
    assert row.is_amount_consistent is False


@pytest.mark.parametrize(
    ("payment_type", "trip_total", "consistent"),
    [
        ("credit card", "16.50", True),   # frais électroniques de 0,50 $
        ("mobile", "17.00", True),        # frais électroniques de 1,00 $
        ("cash", "16.50", False),         # pas de frais attendus en espèces
        ("credit card", "16.75", False),  # écart qui ne correspond à aucun frais connu
    ],
)
def test_electronic_payment_fee_is_consistent(to_bronze_df, payment_type, trip_total, consistent):
    valid, _ = process(to_bronze_df([source_row("a", payment_type=payment_type, trip_total=trip_total)]))
    assert valid.first().is_amount_consistent is consistent


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"trip_id": None}, "missing_trip_id"),
        ({"trip_start_timestamp": "not-a-date"}, "invalid_start_timestamp"),
        ({"trip_end_timestamp": ""}, "invalid_end_timestamp"),
        ({"trip_end_timestamp": "2023-01-01T07:00:00.000"}, "end_before_start"),
        ({"trip_seconds": ""}, "missing_duration"),
        ({"trip_seconds": "-5"}, "invalid_duration"),
        ({"trip_seconds": "100000"}, "invalid_duration"),
        ({"trip_miles": "abc"}, "missing_distance"),
        ({"trip_miles": "900"}, "invalid_distance"),
        ({"trip_seconds": "0", "trip_miles": "0"}, "empty_trip"),
        ({"trip_seconds": "600", "trip_miles": "100"}, "implausible_speed"),
        ({"trip_total": ""}, "missing_amount"),
        ({"fare": "-3"}, "negative_amount"),
        ({"trip_total": "5000"}, "implausible_amount"),
    ],
)
def test_invalid_rows_are_quarantined_with_reason(to_bronze_df, overrides, reason):
    trip_id = overrides.pop("trip_id", "bad")
    valid, rejected = process(to_bronze_df([source_row(trip_id, **overrides), source_row("ok")]))

    assert [r.trip_id for r in valid.collect()] == ["ok"]
    assert [r.reject_reason for r in rejected.collect()] == [reason]
