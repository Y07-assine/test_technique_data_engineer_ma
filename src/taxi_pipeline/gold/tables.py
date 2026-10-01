"""Catalogue des tables gold : grain (clé primaire), relations et nom publié.

Source unique utilisée par le job gold (contrôle du grain), la publication PostgreSQL
(clés primaires et étrangères) et les tests.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from taxi_pipeline.gold.facts import GRAINS

DIMENSION, FACT, KPI = "dimension", "fact", "kpi"


@dataclass(frozen=True)
class ForeignKey:
    columns: tuple[str, ...]
    ref_table: str


@dataclass(frozen=True)
class GoldTable:
    name: str
    kind: str
    primary_key: tuple[str, ...]
    foreign_keys: tuple[ForeignKey, ...] = field(default_factory=tuple)

    @property
    def published_name(self) -> str:
        # Les sorties KPI gardent leurs noms historiques (gold_*) pour les consommateurs BI.
        return f"gold_{self.name}" if self.kind == KPI else self.name


def _fact(name: str, *dimensions: tuple[str, str]) -> GoldTable:
    return GoldTable(
        name,
        FACT,
        tuple(GRAINS[name]),
        tuple(ForeignKey((column,), dim) for column, dim in dimensions),
    )


DATE_FK = ("date_key", "dim_date")

GOLD_TABLES: list[GoldTable] = [
    GoldTable("dim_date", DIMENSION, ("date_key",)),
    GoldTable("dim_zone", DIMENSION, ("zone_key",)),
    GoldTable("dim_payment_type", DIMENSION, ("payment_type_key",)),
    GoldTable("dim_time", DIMENSION, ("time_key",)),
    _fact("fact_daily_metrics", DATE_FK),
    _fact("fact_zone_metrics", DATE_FK, ("zone_key", "dim_zone")),
    _fact("fact_hourly_demand", DATE_FK, ("time_key", "dim_time")),
    _fact("fact_payment_metrics", DATE_FK, ("payment_type_key", "dim_payment_type")),
    GoldTable("daily_metrics", KPI, ("trip_date",)),
    GoldTable("pickup_zone_metrics", KPI, ("rank",)),
    GoldTable("hourly_demand", KPI, ("day_of_week_num", "trip_hour")),
]

TABLES_BY_NAME = {table.name: table for table in GOLD_TABLES}
