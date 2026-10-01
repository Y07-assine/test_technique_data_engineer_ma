from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from taxi_pipeline.gold.dimensions import UNKNOWN_PAYMENT_TYPE, UNKNOWN_ZONE, UNKNOWN_ZONE_KEY
from taxi_pipeline.gold.job import build_gold_model, read_community_areas
from taxi_pipeline.gold.tables import FACT, GOLD_TABLES, KPI, TABLES_BY_NAME
from taxi_pipeline.gold.validation import GoldValidationError, assert_reconciles, assert_unique_grain, totals

TRIPS_SCHEMA = (
    "trip_date date, trip_hour int, taxi_id string, pickup_community_area int, payment_type string, "
    "trip_total decimal(10,2), fare decimal(10,2), tips decimal(10,2), tip_pct double, "
    "trip_minutes double, trip_miles double"
)
# 2023-01-01 (dimanche) et 2023-01-03 (mardi) : le 2 janvier n'a aucun trajet.
D1, D2, D3 = date(2023, 1, 1), date(2023, 1, 2), date(2023, 1, 3)


@pytest.fixture(scope="module")
def trips(spark):
    rows = [
        (D1, 8, "t1", 8, "Credit Card", Decimal("16.00"), Decimal("12.50"), Decimal("2.50"), 20.0, 15.0, 3.5),
        (D1, 8, "t2", 8, "Cash", Decimal("24.00"), Decimal("20.00"), Decimal("4.00"), 20.0, 25.0, 6.0),
        (D1, 9, "t1", 32, "Cash", Decimal("10.00"), Decimal("10.00"), Decimal("0.00"), 0.0, 5.0, 1.0),
        (D3, 8, "t3", None, None, Decimal("50.00"), Decimal("40.00"), Decimal("10.00"), 25.0, 30.0, 15.0),
        # Zone absente du référentiel : doit être rattachée au membre inconnu.
        (D3, 8, "t3", 99, "Mobile", Decimal("20.00"), Decimal("20.00"), Decimal("0.00"), 0.0, 10.0, 2.0),
    ]
    return spark.createDataFrame(rows, TRIPS_SCHEMA)


@pytest.fixture(scope="module")
def model(spark, trips):
    return build_gold_model(trips, read_community_areas(spark))


def rows_by(df, key):
    return {row[key]: row for row in df.collect()}


# --- Dimensions -----------------------------------------------------------------


def test_dim_date_covers_every_day_including_days_without_trips(model):
    dates = rows_by(model["dim_date"], "date")
    assert sorted(dates) == [D1, D2, D3]

    sunday = dates[D1]
    assert sunday.date_key == 20230101
    assert (sunday.year, sunday.quarter, sunday.month, sunday.day_of_month) == (2023, 1, 1, 1)
    assert (sunday.day_of_week, sunday.day_name, sunday.is_weekend) == (7, "Sunday", True)
    assert sunday.week_start == date(2022, 12, 26)  # semaine ISO : commence le lundi
    assert sunday.iso_week == 52
    assert (dates[D2].day_of_week, dates[D2].is_weekend) == (1, False)


def test_dim_zone_has_all_community_areas_and_unknown_member(model):
    zones = rows_by(model["dim_zone"], "zone_key")
    assert len(zones) == 78
    assert zones[32].zone_name == "Loop"
    assert zones[32].community_area == 32
    assert zones[UNKNOWN_ZONE_KEY].zone_name == UNKNOWN_ZONE
    assert zones[UNKNOWN_ZONE_KEY].community_area is None


def test_dim_payment_type_keeps_normalized_values(model):
    payments = rows_by(model["dim_payment_type"], "payment_type")
    assert sorted(payments) == ["Cash", "Credit Card", "Mobile", UNKNOWN_PAYMENT_TYPE]
    assert sorted(p.payment_type_key for p in payments.values()) == [1, 2, 3, 4]


def test_dim_time_has_one_row_per_hour(model):
    hours = rows_by(model["dim_time"], "time_key")
    assert sorted(hours) == list(range(24))
    assert hours[8].hour_label == "08:00"


# --- Grain et réconciliation ------------------------------------------------------


@pytest.mark.parametrize("table", GOLD_TABLES, ids=lambda t: t.name)
def test_every_table_is_unique_at_its_declared_grain(model, table):
    df = model[table.name]
    assert df.count() == df.select(*table.primary_key).distinct().count()
    assert_unique_grain(df, table.primary_key, table.name)


@pytest.mark.parametrize("fact", [t.name for t in GOLD_TABLES if t.kind == FACT])
def test_facts_reconcile_with_silver(model, trips, fact):
    silver = trips.agg(F.count("*"), F.sum("trip_total")).first()
    assert totals(model[fact]) == (silver[0], silver[1])


def test_fact_foreign_keys_all_exist_in_their_dimension(model):
    for table in GOLD_TABLES:
        for fk in table.foreign_keys:
            ref = TABLES_BY_NAME[fk.ref_table]
            assert fk.columns == ref.primary_key  # convention : même nom de clé des deux côtés
            orphans = model[table.name].join(model[ref.name], list(fk.columns), "left_anti")
            assert orphans.count() == 0, f"{table.name}.{fk.columns} -> {ref.name}"


def test_fact_zone_maps_unknown_and_unreferenced_areas_to_unknown_member(model):
    facts = {(r.date_key, r.zone_key): r for r in model["fact_zone_metrics"].collect()}
    assert facts[(20230103, UNKNOWN_ZONE_KEY)].nb_trips == 2  # zone nulle + zone 99
    assert facts[(20230101, 8)].nb_trips == 2


def test_fact_daily_metrics(model):
    d1 = rows_by(model["fact_daily_metrics"], "date_key")[20230101]
    assert (d1.nb_trips, d1.active_taxis) == (3, 2)
    assert d1.total_revenue == Decimal("50.00")
    assert d1.total_trip_minutes == 45.0
    assert d1.avg_revenue_per_trip == pytest.approx(16.67)
    assert d1.median_trip_minutes == 15.0
    assert d1.tip_rate_pct == pytest.approx(6.5 / 42.5 * 100, abs=0.01)


def test_daily_median_is_exact_and_independent_of_row_order(spark):
    """Nombre pair de valeurs : la médiane exacte interpole les deux valeurs centrales."""
    minutes = [30.0, 5.0, 25.0, 15.0]
    base = spark.createDataFrame(
        [(D1, 8, "t", 8, "Cash", Decimal("1.00"), Decimal("1.00"), Decimal("0"), 0.0, m, 1.0) for m in minutes],
        TRIPS_SCHEMA,
    )
    medians = {
        build_gold_model(df, read_community_areas(spark))["fact_daily_metrics"].first().median_trip_minutes
        for df in (base, base.orderBy(F.desc("trip_minutes")), base.repartition(3))
    }
    assert medians == {20.0}


def test_validation_guards_fail_loudly(spark):
    duplicated = spark.createDataFrame([(1, 5), (1, 6)], "date_key int, nb_trips int")
    with pytest.raises(GoldValidationError, match="grain"):
        assert_unique_grain(duplicated, ("date_key",), "fact_x")

    fact = spark.createDataFrame([(1, Decimal("10.00"))], "nb_trips long, total_revenue decimal(14,2)")
    with pytest.raises(GoldValidationError, match="does not reconcile"):
        assert_reconciles({"fact_x": fact}, 2, Decimal("10.00"))


# --- Sorties KPI : équivalence avec le calcul direct sur la silver ------------------


def test_daily_kpi_matches_direct_computation(model, trips):
    kpi = rows_by(model["daily_metrics"], "trip_date")
    assert sorted(kpi) == [D1, D3]  # pas de ligne pour un jour sans trajet
    direct = trips.groupBy("trip_date").agg(F.count("*").alias("n"), F.sum("trip_total").alias("rev"),
                                            F.round(F.avg("trip_minutes"), 2).alias("minutes"))
    for row in direct.collect():
        assert (kpi[row.trip_date].nb_trips, kpi[row.trip_date].total_revenue) == (row.n, row.rev)
        assert kpi[row.trip_date].avg_trip_minutes == row.minutes
    assert kpi[D1].day_of_week == "Sunday"
    assert kpi[D1].week_start == date(2022, 12, 26)


def test_zone_kpi_matches_direct_computation_over_the_period(model, trips):
    """Moyennes de période = somme / somme sur les faits, identiques au calcul par trajet."""
    zones = sorted(model["pickup_zone_metrics"].collect(), key=lambda r: r.rank)
    assert [(z.rank, z.zone_name, z.nb_trips) for z in zones] == [
        (1, "Near North Side", 2),
        (2, UNKNOWN_ZONE, 2),
        (3, "Loop", 1),
    ]
    near_north = zones[0]
    direct = trips.where("pickup_community_area = 8").agg(
        F.round(F.avg("trip_total"), 2).cast("double"), F.round(F.avg("trip_minutes"), 2),
        F.round(F.avg("trip_miles"), 2),
    ).first()
    assert near_north.avg_revenue_per_trip == direct[0]
    assert near_north.avg_trip_minutes == direct[1]
    assert near_north.avg_trip_miles == direct[2]
    assert near_north.total_revenue == Decimal("40.00")
    assert zones[1].community_area is None
    assert sum(z.share_of_trips_pct for z in zones) == pytest.approx(100.0)


def test_hourly_kpi_keeps_historical_semantics(model):
    rows = {(r.day_of_week, r.trip_hour): r for r in model["hourly_demand"].collect()}
    sunday_8 = rows[("Sunday", 8)]
    assert (sunday_8.nb_trips, sunday_8.nb_days, sunday_8.avg_trips_per_day) == (2, 1, 2.0)
    assert sunday_8.day_of_week_num == 1  # numérotation historique : 1 = dimanche
    assert rows[("Tuesday", 8)].day_of_week_num == 3
    assert rows[("Tuesday", 8)].avg_trip_minutes == 20.0
    assert len(rows) == 3


def test_kpi_outputs_keep_historical_published_names():
    assert sorted(t.published_name for t in GOLD_TABLES if t.kind == KPI) == [
        "gold_daily_metrics", "gold_hourly_demand", "gold_pickup_zone_metrics",
    ]
