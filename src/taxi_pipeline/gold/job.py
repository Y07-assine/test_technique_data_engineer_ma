from __future__ import annotations

from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from taxi_pipeline.config import PipelineConfig
from taxi_pipeline.gold import dimensions, facts, kpis
from taxi_pipeline.gold.tables import FACT, GOLD_TABLES
from taxi_pipeline.gold.validation import assert_reconciles, assert_unique_grain

SEED_COMMUNITY_AREAS = Path(__file__).resolve().parent.parent / "seeds" / "community_areas.csv"

# Projection explicite : Parquet étant colonnaire, seules ces colonnes sont lues.
SILVER_COLUMNS = ["trip_date", "trip_hour", "taxi_id", "pickup_community_area", "payment_type",
                  "trip_total", "fare", "tips", "tip_pct", "trip_minutes", "trip_miles"]


def read_community_areas(spark: SparkSession) -> DataFrame:
    return (
        spark.read.option("header", "true")
        .schema("community_area int, community_area_name string, side string")
        .csv(str(SEED_COMMUNITY_AREAS))
    )


def build_gold_model(trips: DataFrame, community_areas: DataFrame) -> dict[str, DataFrame]:
    """Dimensions, faits, puis sorties KPI dérivées des faits (ordre de dépendance)."""
    model = {
        "dim_date": dimensions.dim_date(trips),
        "dim_zone": dimensions.dim_zone(community_areas),
        "dim_payment_type": dimensions.dim_payment_type(trips),
        "dim_time": dimensions.dim_time(trips.sparkSession),
    }
    model["fact_daily_metrics"] = facts.fact_daily_metrics(trips)
    model["fact_zone_metrics"] = facts.fact_zone_metrics(trips, model["dim_zone"])
    model["fact_hourly_demand"] = facts.fact_hourly_demand(trips)
    model["fact_payment_metrics"] = facts.fact_payment_metrics(trips, model["dim_payment_type"])
    model["daily_metrics"] = kpis.daily_metrics(model["fact_daily_metrics"], model["dim_date"])
    model["pickup_zone_metrics"] = kpis.pickup_zone_metrics(model["fact_zone_metrics"], model["dim_zone"])
    model["hourly_demand"] = kpis.hourly_demand(model["fact_hourly_demand"], model["dim_date"])
    return model


def run_gold(spark: SparkSession, config: PipelineConfig) -> dict:
    """Recalcule le modèle gold complet à partir de toute la couche silver.

    Les classements et parts dépendent de toute la donnée : un recalcul complet est
    simple et reste peu coûteux à cette volumétrie (toutes les tables gold réunies
    font quelques milliers de lignes).
    """
    trips = spark.read.parquet(config.silver_trips).select(*SILVER_COLUMNS).persist(StorageLevel.MEMORY_AND_DISK)
    try:
        summary = trips.agg(F.count("*").alias("rows"), F.sum("trip_total").alias("revenue"),
                            F.min("trip_date").alias("first"), F.max("trip_date").alias("last")).first()
        metrics = {"silver_rows_used": summary["rows"], "gold_period": f"{summary['first']}..{summary['last']}"}

        # Les tables sont matérialisées (cache) : validation puis écriture sans recalcul.
        model = {name: df.persist() for name, df in build_gold_model(trips, read_community_areas(spark)).items()}
        for table in GOLD_TABLES:
            metrics[f"{table.name}_rows"] = assert_unique_grain(model[table.name], table.primary_key, table.name)
        assert_reconciles({t.name: model[t.name] for t in GOLD_TABLES if t.kind == FACT},
                          summary["rows"], summary["revenue"])

        for table in GOLD_TABLES:
            # Tables de quelques lignes à quelques milliers : un seul fichier chacune.
            model[table.name].coalesce(1).write.mode("overwrite").parquet(config.gold(table.name))
        for df in model.values():
            df.unpersist()
    finally:
        trips.unpersist()
    return metrics
