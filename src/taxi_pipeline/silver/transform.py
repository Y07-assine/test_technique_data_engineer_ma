"""BRONZE -> SILVER : typage explicite, normalisation, dédoublonnage, validation.

Fonctions pures sur DataFrames (sans I/O) pour être testables unitairement.
Les lignes invalides ne sont jamais supprimées silencieusement : elles reçoivent
une `reject_reason` et sont isolées en quarantaine.
"""

from __future__ import annotations

import operator
from functools import reduce

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS"

# Bornes de plausibilité (valeurs aberrantes évidentes).
MAX_TRIP_SECONDS = 24 * 3600
MAX_TRIP_MILES = 500.0
MAX_TRIP_TOTAL = 1_000.0
MAX_SPEED_MPH = 90.0
MIN_SECONDS_FOR_SPEED_CHECK = 60
AMOUNT_TOLERANCE = 0.05
# Constaté sur la source : trip_total inclut des frais fixes non détaillés pour les paiements
# électroniques (0,50 $ le plus souvent, parfois 1,00 $), absents de fare/tips/tolls/extras.
ELECTRONIC_PAYMENTS = ["Credit Card", "Mobile"]
ELECTRONIC_PAYMENT_FEES = [0.50, 1.00]

MONEY_COLUMNS = ["fare", "tips", "tolls", "extras", "trip_total"]
COORDINATE_COLUMNS = [
    "pickup_centroid_latitude",
    "pickup_centroid_longitude",
    "dropoff_centroid_latitude",
    "dropoff_centroid_longitude",
]

REQUIRED_COLUMNS = ["trip_id", "trip_start_ts", "trip_end_ts", "trip_seconds", "trip_miles", "fare", "trip_total"]


def _clean_string(col: str) -> Column:
    trimmed = F.trim(F.col(col))
    return F.when(trimmed == "", None).otherwise(trimmed)


def cast_columns(bronze: DataFrame) -> DataFrame:
    """Schéma cible typé. Les colonnes POINT(...) sont redondantes avec lat/lon : abandonnées."""
    return bronze.select(
        _clean_string("trip_id").alias("trip_id"),
        _clean_string("taxi_id").alias("taxi_id"),
        # Horodatages « muraux » de Chicago, sans offset dans la source : TIMESTAMP_NTZ les
        # conserve tels quels, indépendamment du fuseau de la session Spark ou du poste.
        F.to_timestamp_ntz("trip_start_timestamp", F.lit(TIMESTAMP_FORMAT)).alias("trip_start_ts"),
        F.to_timestamp_ntz("trip_end_timestamp", F.lit(TIMESTAMP_FORMAT)).alias("trip_end_ts"),
        _clean_string("trip_seconds").cast("long").alias("trip_seconds"),
        _clean_string("trip_miles").cast("double").alias("trip_miles"),
        _clean_string("pickup_census_tract").alias("pickup_census_tract"),
        _clean_string("dropoff_census_tract").alias("dropoff_census_tract"),
        _clean_string("pickup_community_area").cast("int").alias("pickup_community_area"),
        _clean_string("dropoff_community_area").cast("int").alias("dropoff_community_area"),
        *[_clean_string(c).cast("decimal(10,2)").alias(c) for c in MONEY_COLUMNS],
        F.initcap(_clean_string("payment_type")).alias("payment_type"),
        _clean_string("company").alias("company"),
        *[_clean_string(c).cast("double").alias(c) for c in COORDINATE_COLUMNS],
        F.col("trip_date").cast("date").alias("source_trip_date"),
        F.col("_source_file").alias("source_file"),
    )


def deduplicate(trips: DataFrame) -> DataFrame:
    """Un trajet par `trip_id` (les doublons de l'API sont des copies identiques)."""
    return trips.dropDuplicates(["trip_id"])


def reject_reason() -> Column:
    """Première règle violée par la ligne, ou null si elle est valide. L'ordre compte."""
    speed_mph = F.col("trip_miles") / (F.col("trip_seconds") / 3600)
    rules = [
        (F.col("trip_id").isNull(), "missing_trip_id"),
        (F.col("trip_start_ts").isNull(), "invalid_start_timestamp"),
        (F.col("trip_end_ts").isNull(), "invalid_end_timestamp"),
        (F.col("trip_end_ts") < F.col("trip_start_ts"), "end_before_start"),
        (F.col("trip_seconds").isNull(), "missing_duration"),
        (~F.col("trip_seconds").between(0, MAX_TRIP_SECONDS), "invalid_duration"),
        (F.col("trip_miles").isNull(), "missing_distance"),
        (~F.col("trip_miles").between(0, MAX_TRIP_MILES), "invalid_distance"),
        ((F.col("trip_seconds") == 0) & (F.col("trip_miles") == 0), "empty_trip"),
        (
            (F.col("trip_seconds") >= MIN_SECONDS_FOR_SPEED_CHECK) & (speed_mph > MAX_SPEED_MPH),
            "implausible_speed",
        ),
        (F.col("fare").isNull() | F.col("trip_total").isNull(), "missing_amount"),
        ((F.col("fare") < 0) | (F.col("trip_total") < 0), "negative_amount"),
        (F.col("trip_total") > MAX_TRIP_TOTAL, "implausible_amount"),
    ]
    expr = F.when(*rules[0])
    for condition, reason in rules[1:]:
        expr = expr.when(condition, reason)
    return expr.otherwise(F.lit(None).cast("string"))


def flag(trips: DataFrame) -> DataFrame:
    return trips.withColumn("reject_reason", reject_reason())


def amount_gap() -> Column:
    """Écart entre trip_total et la somme de ses composantes publiées."""
    components = reduce(operator.add, [F.coalesce(F.col(c), F.lit(0)) for c in ["fare", "tips", "tolls", "extras"]])
    return F.col("trip_total") - components


def is_amount_consistent(gap: Column) -> Column:
    """Écart nul, ou égal à des frais connus de paiement électronique."""
    def near(value: float) -> Column:
        return F.abs(gap - F.lit(value)) <= AMOUNT_TOLERANCE

    known_fee = reduce(operator.or_, [near(fee) for fee in ELECTRONIC_PAYMENT_FEES])
    return near(0.0) | (F.col("payment_type").isin(ELECTRONIC_PAYMENTS) & known_fee)


def enrich(valid: DataFrame) -> DataFrame:
    """Colonnes dérivées pour l'analyse."""
    return valid.select(
        "*",
        F.to_date("trip_start_ts").alias("trip_date"),
        F.hour("trip_start_ts").alias("trip_hour"),
        F.dayofweek("trip_start_ts").alias("day_of_week_num"),
        F.date_format("trip_start_ts", "EEEE").alias("day_of_week"),
        F.round(F.col("trip_seconds") / 60, 2).alias("trip_minutes"),
        F.when(
            F.col("trip_seconds") > 0,
            F.round(F.col("trip_miles") / (F.col("trip_seconds") / 3600), 2),
        ).alias("avg_speed_mph"),
        F.when(F.col("fare") > 0, F.round(F.col("tips") / F.col("fare") * 100, 2).cast("double")).alias("tip_pct"),
        # La source ne garantit pas trip_total = somme des composantes : on le mesure et
        # on le signale sans rejeter la ligne.
        amount_gap().alias("amount_gap"),
        is_amount_consistent(amount_gap()).alias("is_amount_consistent"),
    )


def split_valid_rejected(flagged: DataFrame) -> tuple[DataFrame, DataFrame]:
    valid = enrich(flagged.where(F.col("reject_reason").isNull()).drop("reject_reason"))
    rejected = flagged.where(F.col("reject_reason").isNotNull()).withColumn("rejected_at", F.current_timestamp())
    return valid, rejected
