"""RAW (CSV de l'API) -> BRONZE (Parquet).

Aucune transformation métier : toutes les colonnes restent des chaînes, exactement
comme reçues. On ajoute uniquement des colonnes techniques de lignage
(`_source_file`, `_ingested_at`) et la colonne de partition `trip_date`.
Le passage en Parquet apporte compression et lecture colonne pour les étapes suivantes.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from taxi_pipeline.config import PipelineConfig
from taxi_pipeline.ingestion.soda import SUCCESS_MARKER, daterange, parse_marker, raw_day_prefix
from taxi_pipeline.utils.storage import ObjectStore

SOURCE_COLUMNS = [
    "trip_id",
    "taxi_id",
    "trip_start_timestamp",
    "trip_end_timestamp",
    "trip_seconds",
    "trip_miles",
    "pickup_census_tract",
    "dropoff_census_tract",
    "pickup_community_area",
    "dropoff_community_area",
    "fare",
    "tips",
    "tolls",
    "extras",
    "trip_total",
    "payment_type",
    "company",
    "pickup_centroid_latitude",
    "pickup_centroid_longitude",
    "pickup_centroid_location",
    "dropoff_centroid_latitude",
    "dropoff_centroid_longitude",
    "dropoff_centroid_location",
]
SOURCE_SCHEMA = StructType([StructField(name, StringType(), True) for name in SOURCE_COLUMNS])


class MissingRawDataError(RuntimeError):
    pass


def raw_manifest(store: ObjectStore, config: PipelineConfig) -> dict[date, int]:
    """Nombre de pages par jour, lu dans les marqueurs `_SUCCESS`.

    Tous les jours de la période doivent avoir été téléchargés entièrement.
    """
    manifest, missing = {}, []
    for day in daterange(config.start_date, config.end_date):
        marker = f"{raw_day_prefix(config.raw_trips, day)}/{SUCCESS_MARKER}"
        if not store.exists(marker):
            missing.append(day)
            continue
        manifest[day] = parse_marker(store.read_bytes(marker).decode())["pages"]
    if missing:
        raise MissingRawDataError(
            f"{len(missing)} day(s) not ingested (no {SUCCESS_MARKER} marker), e.g. {missing[:5]}. "
            "Run the download step first."
        )
    return manifest


def read_raw(spark: SparkSession, config: PipelineConfig, days: list[date]) -> DataFrame:
    paths = [f"{raw_day_prefix(config.raw_trips, d)}/page_*.csv" for d in days]
    return (
        spark.read.option("header", "true")
        # Le header est vérifié contre le schéma attendu : une dérive de schéma
        # côté source fait échouer la lecture au lieu de décaler les colonnes.
        .option("enforceSchema", "false")
        .option("escape", '"')
        .option("multiLine", "true")
        .option("mode", "FAILFAST")
        .schema(SOURCE_SCHEMA)
        .csv(paths)
    )


def to_bronze(raw: DataFrame) -> DataFrame:
    return (
        raw.withColumn("_source_file", F.input_file_name())
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("trip_date", F.regexp_extract("_source_file", r"trip_date=(\d{4}-\d{2}-\d{2})", 1))
    )


def run_bronze(spark: SparkSession, store: ObjectStore, config: PipelineConfig) -> dict:
    manifest = raw_manifest(store, config)
    existing_days = [day for day, pages in manifest.items() if pages > 0]
    if not existing_days:
        raise MissingRawDataError("The requested period contains no trip at all.")

    bronze = to_bronze(read_raw(spark, config, existing_days))
    # Un fichier par jour : ~10-20k lignes / jour, taille de fichier raisonnable,
    # et un jour peut être rejoué indépendamment (overwrite dynamique de partition).
    (
        bronze.repartition("trip_date")
        .write.mode("overwrite")
        .partitionBy("trip_date")
        .parquet(config.bronze_trips)
    )
    rows = spark.read.parquet(config.bronze_trips).where(_in_period(config)).count()
    return {"days": len(existing_days), "bronze_rows": rows}


def _in_period(config: PipelineConfig):
    return F.col("trip_date").between(F.lit(config.start_date), F.lit(config.end_date))
