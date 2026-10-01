from __future__ import annotations

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from taxi_pipeline.config import PipelineConfig
from taxi_pipeline.silver.transform import cast_columns, deduplicate, flag, split_valid_rejected


def period_filter(config: PipelineConfig, column: str = "trip_date"):
    return F.col(column).between(F.lit(config.start_date), F.lit(config.end_date))


def read_bronze(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    # Le filtre sur la colonne de partition est poussé à la lecture (partition pruning).
    return spark.read.parquet(config.bronze_trips).where(period_filter(config))


def run_silver(spark: SparkSession, config: PipelineConfig) -> dict:
    bronze = read_bronze(spark, config)
    typed = cast_columns(bronze)
    # Persisté car réutilisé par trois actions (comptage, écriture valides, écriture rejets).
    flagged = flag(deduplicate(typed)).persist(StorageLevel.MEMORY_AND_DISK)

    try:
        counts = {
            (row["reject_reason"] or "valid"): row["count"]
            for row in flagged.groupBy("reject_reason").count().collect()
        }
        valid, rejected = split_valid_rejected(flagged)

        # Overwrite dynamique : seules les partitions (jours) de la période sont remplacées.
        (
            valid.repartition("trip_date")
            .write.mode("overwrite")
            .partitionBy("trip_date")
            .parquet(config.silver_trips)
        )
        (
            rejected.withColumnRenamed("source_trip_date", "trip_date")
            .repartition("trip_date")
            .write.mode("overwrite")
            .partitionBy("trip_date")
            .parquet(config.silver_rejected_trips)
        )
    finally:
        flagged.unpersist()

    bronze_rows = bronze.count()
    deduplicated_rows = sum(counts.values())
    valid_rows = counts.get("valid", 0)
    return {
        "bronze_rows": bronze_rows,
        "duplicates_removed": bronze_rows - deduplicated_rows,
        "silver_rows": valid_rows,
        "rejected_rows": deduplicated_rows - valid_rows,
        "rejects_by_reason": {k: v for k, v in sorted(counts.items()) if k != "valid"},
    }
