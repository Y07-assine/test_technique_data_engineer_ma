"""Dimensions du modèle gold : attributs descriptifs réutilisables par tous les faits.

Les dimensions sont reconstruites à chaque run (full refresh, comme les faits) : leurs
clés sont donc toujours cohérentes avec les faits publiés en même temps. Pas de SCD :
les attributs (calendrier, zones, moyens de paiement) ne changent pas dans le temps.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

UNKNOWN_ZONE_KEY = -1
UNKNOWN_ZONE = "Unknown / outside Chicago"
UNKNOWN_PAYMENT_TYPE = "Unknown"


def date_key(date_col: str | Column) -> Column:
    """Clé entière YYYYMMDD : lisible, triable, stable d'un run à l'autre."""
    return F.date_format(date_col, "yyyyMMdd").cast("int")


def iso_day_of_week(date_col: str | Column) -> Column:
    """1 = lundi … 7 = dimanche (Spark `dayofweek` commence le dimanche)."""
    return ((F.dayofweek(date_col) + 5) % 7) + 1


def dim_date(trips: DataFrame) -> DataFrame:
    """Une ligne par jour entre la première et la dernière date de la silver.

    Générée par `sequence` : un jour sans trajet resterait présent dans le calendrier.
    """
    bounds = trips.agg(F.min("trip_date").alias("first"), F.max("trip_date").alias("last"))
    return (
        bounds.select(F.explode(F.sequence("first", "last", F.expr("interval 1 day"))).alias("date"))
        .select(
            date_key("date").alias("date_key"),
            "date",
            F.year("date").alias("year"),
            F.quarter("date").alias("quarter"),
            F.month("date").alias("month"),
            F.date_format("date", "MMMM").alias("month_name"),
            F.weekofyear("date").alias("iso_week"),
            F.date_trunc("week", "date").cast("date").alias("week_start"),
            F.dayofmonth("date").alias("day_of_month"),
            iso_day_of_week("date").alias("day_of_week"),
            F.date_format("date", "EEEE").alias("day_name"),
            (iso_day_of_week("date") >= 6).alias("is_weekend"),
        )
    )


def dim_zone(community_areas: DataFrame) -> DataFrame:
    """Les 77 community areas (clé = numéro officiel) + un membre « inconnu » (clé -1)."""
    spark = community_areas.sparkSession
    unknown = spark.createDataFrame(
        [(UNKNOWN_ZONE_KEY, None, UNKNOWN_ZONE, UNKNOWN_ZONE)],
        "zone_key int, community_area int, zone_name string, side string",
    )
    known = community_areas.select(
        F.col("community_area").alias("zone_key"),
        "community_area",
        F.col("community_area_name").alias("zone_name"),
        "side",
    )
    return known.unionByName(unknown)


def dim_payment_type(trips: DataFrame) -> DataFrame:
    """Moyens de paiement observés dans la silver (valeurs déjà normalisées)."""
    return (
        trips.select(F.coalesce("payment_type", F.lit(UNKNOWN_PAYMENT_TYPE)).alias("payment_type"))
        .distinct()
        # ~10 lignes : une fenêtre globale ne coûte rien et donne des clés déterministes.
        .withColumn("payment_type_key", F.row_number().over(Window.orderBy("payment_type")))
        .select("payment_type_key", "payment_type")
    )


def dim_time(spark: SparkSession) -> DataFrame:
    """Heure de la journée (24 lignes). Le jour de semaine est un attribut de dim_date."""
    return spark.range(24).select(
        F.col("id").cast("int").alias("time_key"),
        F.col("id").cast("int").alias("hour"),
        F.format_string("%02d:00", F.col("id").cast("int")).alias("hour_label"),
    )
