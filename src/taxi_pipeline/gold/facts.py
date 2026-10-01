"""Faits du modèle gold : mesures agrégées à un grain explicite.

Chaque fait porte des mesures additives (comptes, sommes) en plus des moyennes :
une moyenne sur une période plus large se recalcule exactement (somme / somme),
jamais en moyennant des moyennes.

| Fait                  | Grain                          | Dimensions            |
|-----------------------|--------------------------------|-----------------------|
| fact_daily_metrics    | date                           | dim_date              |
| fact_zone_metrics     | date x zone de prise en charge | dim_date, dim_zone    |
| fact_hourly_demand    | date x heure                   | dim_date, dim_time    |
| fact_payment_metrics  | date x moyen de paiement       | dim_date, dim_payment_type |
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from taxi_pipeline.gold.dimensions import UNKNOWN_PAYMENT_TYPE, UNKNOWN_ZONE_KEY, date_key

GRAINS = {
    "fact_daily_metrics": ["date_key"],
    "fact_zone_metrics": ["date_key", "zone_key"],
    "fact_hourly_demand": ["date_key", "time_key"],
    "fact_payment_metrics": ["date_key", "payment_type_key"],
}


def _additive_measures() -> list:
    return [
        F.count("*").alias("nb_trips"),
        F.sum("trip_total").cast("decimal(14,2)").alias("total_revenue"),
        F.sum("trip_minutes").alias("total_trip_minutes"),
        F.sum("trip_miles").alias("total_trip_miles"),
    ]


def _average_measures() -> list:
    return [
        F.round(F.avg("trip_total"), 2).cast("double").alias("avg_revenue_per_trip"),
        F.round(F.avg("trip_minutes"), 2).alias("avg_trip_minutes"),
        F.round(F.avg("trip_miles"), 2).alias("avg_trip_miles"),
    ]


def fact_daily_metrics(trips: DataFrame) -> DataFrame:
    """Médiane et taxis distincts ne sont pas additifs : ils sont calculés ici, au grain jour."""
    return (
        trips.groupBy(date_key("trip_date").alias("date_key"))
        .agg(
            *_additive_measures(),
            *_average_measures(),
            # Médiane exacte : percentile_approx dépend de l'ordre de lecture des lignes et
            # donnait des résultats différents d'un run à l'autre. ~16 000 valeurs par jour :
            # le calcul exact ne coûte rien.
            F.round(F.percentile("trip_minutes", 0.5), 2).alias("median_trip_minutes"),
            F.sum("fare").cast("decimal(14,2)").alias("total_fare"),
            F.sum("tips").cast("decimal(14,2)").alias("total_tips"),
            F.countDistinct("taxi_id").alias("active_taxis"),
        )
        .withColumn("tip_rate_pct", F.round(F.col("total_tips") / F.col("total_fare") * 100, 2).cast("double"))
    )


def fact_zone_metrics(trips: DataFrame, dim_zone: DataFrame) -> DataFrame:
    # dim_zone (78 lignes) est diffusée : pas de shuffle de la table des trajets.
    # Une zone absente du référentiel (ou nulle) est rattachée au membre inconnu.
    zones = F.broadcast(dim_zone.select("zone_key"))
    return (
        trips.join(zones, trips["pickup_community_area"] == zones["zone_key"], "left")
        .groupBy(
            date_key("trip_date").alias("date_key"),
            F.coalesce("zone_key", F.lit(UNKNOWN_ZONE_KEY)).alias("zone_key"),
        )
        .agg(*_additive_measures(), *_average_measures())
    )


def fact_hourly_demand(trips: DataFrame) -> DataFrame:
    return (
        trips.groupBy(date_key("trip_date").alias("date_key"), F.col("trip_hour").alias("time_key"))
        .agg(*_additive_measures(), *_average_measures())
    )


def fact_payment_metrics(trips: DataFrame, dim_payment_type: DataFrame) -> DataFrame:
    payments = F.broadcast(dim_payment_type)
    return (
        trips.withColumn("payment_type", F.coalesce("payment_type", F.lit(UNKNOWN_PAYMENT_TYPE)))
        .join(payments, "payment_type")
        .groupBy(date_key("trip_date").alias("date_key"), "payment_type_key")
        .agg(
            *_additive_measures(),
            F.round(F.avg("trip_total"), 2).cast("double").alias("avg_revenue_per_trip"),
            F.sum("tips").cast("decimal(14,2)").alias("total_tips"),
            F.round(F.avg("tip_pct"), 2).alias("avg_tip_pct"),
        )
    )
