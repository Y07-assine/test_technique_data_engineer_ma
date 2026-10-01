"""Configuration centralisée du pipeline, lue depuis les variables d'environnement.

Chaque valeur a un défaut adapté à l'environnement Docker Compose, ce qui permet
de lancer le projet sans .env. `DATALAKE_ROOT` accepte aussi un chemin local
(ex. `/tmp/datalake`), utilisé par les tests et l'exécution hors Docker.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value not in (None, "") else default


@dataclass(frozen=True)
class SodaConfig:
    dataset_url: str = "https://data.cityofchicago.org/resource/wrvz-psew.csv"
    app_token: str | None = None
    page_size: int = 50_000
    timeout_seconds: int = 120
    max_retries: int = 5
    max_rows_per_day: int | None = None


@dataclass(frozen=True)
class S3Config:
    endpoint: str = "http://minio:9000"
    access_key: str = "minioadmin"
    secret_key: str = "minioadmin"


@dataclass(frozen=True)
class SparkConfig:
    master: str = "local[*]"
    driver_memory: str = "2g"
    shuffle_partitions: int = 8


@dataclass(frozen=True)
class AnalyticsDbConfig:
    host: str = "postgres"
    port: int = 5432
    database: str = "analytics"
    user: str = "analytics"
    password: str = "analytics"

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"


@dataclass(frozen=True)
class QualityConfig:
    max_reject_rate: float = 0.10
    max_optional_null_rate: float = 0.25
    min_amount_consistency_rate: float = 0.95


@dataclass(frozen=True)
class PipelineConfig:
    datalake_root: str = "s3a://datalake"
    start_date: date = date(2023, 1, 1)
    end_date: date = date(2023, 3, 31)
    soda: SodaConfig = field(default_factory=SodaConfig)
    s3: S3Config = field(default_factory=S3Config)
    spark: SparkConfig = field(default_factory=SparkConfig)
    analytics_db: AnalyticsDbConfig = field(default_factory=AnalyticsDbConfig)
    quality: QualityConfig = field(default_factory=QualityConfig)

    # --- Chemins des couches du datalake ---
    def path(self, *parts: str) -> str:
        return "/".join([self.datalake_root.rstrip("/"), *parts])

    @property
    def raw_trips(self) -> str:
        return self.path("raw", "taxi_trips")

    @property
    def bronze_trips(self) -> str:
        return self.path("bronze", "taxi_trips")

    @property
    def silver_trips(self) -> str:
        return self.path("silver", "taxi_trips")

    @property
    def silver_rejected_trips(self) -> str:
        return self.path("silver", "taxi_trips_rejected")

    def gold(self, table: str) -> str:
        return self.path("gold", table)

    @property
    def quality_reports(self) -> str:
        return self.path("quality_reports")

    def with_period(self, start: date, end: date) -> "PipelineConfig":
        if start > end:
            raise ValueError(f"start_date ({start}) must be <= end_date ({end})")
        return PipelineConfig(
            datalake_root=self.datalake_root,
            start_date=start,
            end_date=end,
            soda=self.soda,
            s3=self.s3,
            spark=self.spark,
            analytics_db=self.analytics_db,
            quality=self.quality,
        )


def load_config() -> PipelineConfig:
    max_rows = os.getenv("SODA_MAX_ROWS_PER_DAY")
    config = PipelineConfig(
        datalake_root=_env("DATALAKE_ROOT", "s3a://datalake"),
        start_date=date.fromisoformat(_env("TRIPS_START_DATE", "2023-01-01")),
        end_date=date.fromisoformat(_env("TRIPS_END_DATE", "2023-03-31")),
        soda=SodaConfig(
            dataset_url=_env("SODA_DATASET_URL", SodaConfig.dataset_url),
            app_token=os.getenv("SODA_APP_TOKEN") or None,
            page_size=int(_env("SODA_PAGE_SIZE", "50000")),
            timeout_seconds=int(_env("SODA_TIMEOUT_SECONDS", "120")),
            max_retries=int(_env("SODA_MAX_RETRIES", "5")),
            max_rows_per_day=int(max_rows) if max_rows else None,
        ),
        s3=S3Config(
            endpoint=_env("S3_ENDPOINT", "http://minio:9000"),
            access_key=_env("S3_ACCESS_KEY", "minioadmin"),
            secret_key=_env("S3_SECRET_KEY", "minioadmin"),
        ),
        spark=SparkConfig(
            master=_env("SPARK_MASTER", "local[*]"),
            driver_memory=_env("SPARK_DRIVER_MEMORY", "2g"),
            shuffle_partitions=int(_env("SPARK_SHUFFLE_PARTITIONS", "8")),
        ),
        analytics_db=AnalyticsDbConfig(
            host=_env("ANALYTICS_DB_HOST", "postgres"),
            port=int(_env("ANALYTICS_DB_PORT", "5432")),
            database=_env("ANALYTICS_DB_NAME", "analytics"),
            user=_env("ANALYTICS_DB_USER", "analytics"),
            password=_env("ANALYTICS_DB_PASSWORD", "analytics"),
        ),
        quality=QualityConfig(
            max_reject_rate=float(_env("QUALITY_MAX_REJECT_RATE", "0.10")),
            max_optional_null_rate=float(_env("QUALITY_MAX_OPTIONAL_NULL_RATE", "0.25")),
            min_amount_consistency_rate=float(_env("QUALITY_MIN_AMOUNT_CONSISTENCY_RATE", "0.95")),
        ),
    )
    return config.with_period(config.start_date, config.end_date)
