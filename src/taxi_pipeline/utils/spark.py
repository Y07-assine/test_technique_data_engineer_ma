from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from pyspark.sql import SparkSession

from taxi_pipeline.config import PipelineConfig

# Les trajets sont typés en TIMESTAMP_NTZ (voir silver) ; le fuseau de session ne sert
# qu'aux colonnes techniques (`_ingested_at`, `rejected_at`). UTC évite toute ambiguïté.
SESSION_TIMEZONE = "UTC"


def build_spark(config: PipelineConfig, app_name: str) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master(config.spark.master)
        .config("spark.driver.memory", config.spark.driver_memory)
        .config("spark.sql.session.timeZone", SESSION_TIMEZONE)
        .config("spark.sql.shuffle.partitions", str(config.spark.shuffle_partitions))
        # Réécriture d'une période = remplacement de ses seules partitions (rejouable).
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.ui.showConsoleProgress", "false")
    )
    if config.datalake_root.startswith("s3a://"):
        builder = (
            builder.config("spark.hadoop.fs.s3a.endpoint", config.s3.endpoint)
            .config("spark.hadoop.fs.s3a.access.key", config.s3.access_key)
            .config("spark.hadoop.fs.s3a.secret.key", config.s3.secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(config.s3.endpoint.startswith("https")).lower())
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            # Les écritures Spark passent par un _temporary : sur S3 le renommage est une copie,
            # l'algorithme v2 limite ce coût.
            .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


@contextmanager
def spark_session(config: PipelineConfig, app_name: str) -> Iterator[SparkSession]:
    """SparkSession à durée de vie d'une tâche : la JVM est libérée en fin de step."""
    spark = build_spark(config, app_name)
    try:
        yield spark
    finally:
        spark.stop()
