"""Sorties KPI orientées BI, dérivées uniquement des faits et des dimensions.

Elles conservent exactement le nom, le schéma et la sémantique des tables gold
historiques (`gold_daily_metrics`, `gold_pickup_zone_metrics`, `gold_hourly_demand`) :
les consommateurs BI existants ne sont pas impactés par le passage au modèle dimensionnel.
Les moyennes de période sont recalculées à partir des mesures additives (somme / somme).
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F


def _ratio(numerator: str, denominator: str = "nb_trips", scale: int = 2) -> Column:
    return F.round(F.col(numerator) / F.col(denominator), scale).cast("double")


def daily_metrics(fact_daily: DataFrame, dim_date: DataFrame) -> DataFrame:
    return fact_daily.join(dim_date, "date_key").select(
        F.col("date").alias("trip_date"),
        F.col("day_name").alias("day_of_week"),
        "nb_trips",
        "total_revenue",
        "avg_revenue_per_trip",
        "avg_trip_minutes",
        "avg_trip_miles",
        "median_trip_minutes",
        "total_fare",
        "total_tips",
        "active_taxis",
        "tip_rate_pct",
        "week_start",
    )


def pickup_zone_metrics(fact_zone: DataFrame, dim_zone: DataFrame) -> DataFrame:
    """Classement des zones sur toute la période."""
    zones = (
        fact_zone.groupBy("zone_key")
        .agg(
            F.sum("nb_trips").alias("nb_trips"),
            F.sum("total_revenue").alias("total_revenue"),
            F.sum("total_trip_minutes").alias("total_trip_minutes"),
            F.sum("total_trip_miles").alias("total_trip_miles"),
        )
        .join(dim_zone, "zone_key")
    )
    return zones.select(
        "community_area",
        "zone_name",
        "side",
        "nb_trips",
        F.col("total_revenue").cast("decimal(14,2)").alias("total_revenue"),
        _ratio("total_revenue").alias("avg_revenue_per_trip"),
        _ratio("total_trip_minutes").alias("avg_trip_minutes"),
        _ratio("total_trip_miles").alias("avg_trip_miles"),
        F.round(F.col("nb_trips") / F.sum("nb_trips").over(Window.partitionBy()) * 100, 2).alias("share_of_trips_pct"),
        F.row_number().over(Window.orderBy(F.desc("nb_trips"), "zone_name")).alias("rank"),
    )


def hourly_demand(fact_hourly: DataFrame, dim_date: DataFrame) -> DataFrame:
    """Profil de demande jour de semaine x heure.

    `nb_days` = nombre de jours ayant au moins un trajet sur ce créneau, soit le nombre
    de lignes du fait (grain date x heure) : même définition que la table historique.
    """
    return (
        fact_hourly.join(dim_date, "date_key")
        .groupBy("day_of_week", "day_name", F.col("time_key").alias("trip_hour"))
        .agg(
            F.sum("nb_trips").alias("nb_trips"),
            F.count("*").alias("nb_days"),
            F.sum("total_revenue").alias("total_revenue"),
            F.sum("total_trip_minutes").alias("total_trip_minutes"),
        )
        .select(
            # Numérotation historique conservée (1 = dimanche), dim_date suivant la norme ISO.
            ((F.col("day_of_week") % 7) + 1).alias("day_of_week_num"),
            F.col("day_name").alias("day_of_week"),
            "trip_hour",
            "nb_trips",
            "nb_days",
            _ratio("total_trip_minutes").alias("avg_trip_minutes"),
            _ratio("total_revenue").alias("avg_revenue_per_trip"),
            F.round(F.col("nb_trips") / F.col("nb_days"), 1).alias("avg_trips_per_day"),
        )
    )
