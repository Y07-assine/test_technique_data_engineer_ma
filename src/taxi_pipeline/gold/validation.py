"""Garde-fous du modèle gold, exécutés avant toute écriture.

- unicité du grain déclaré de chaque table ;
- réconciliation des faits avec la silver (nombre de trajets et revenu total).
Toute violation fait échouer la tâche gold : rien n'est écrit ni publié.
"""

from __future__ import annotations

import operator
from decimal import Decimal
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class GoldValidationError(RuntimeError):
    pass


def assert_unique_grain(df: DataFrame, keys: tuple[str, ...], table: str) -> int:
    stats = df.agg(
        F.count("*").alias("rows"),
        F.count_distinct(*[F.col(k) for k in keys]).alias("distinct_keys"),
        F.sum(reduce(operator.or_, [F.col(k).isNull() for k in keys]).cast("int")).alias("null_keys"),
    ).first()
    if stats["rows"] != stats["distinct_keys"] or stats["null_keys"]:
        raise GoldValidationError(
            f"{table}: grain {keys} violated (rows={stats['rows']}, distinct={stats['distinct_keys']}, "
            f"null keys={stats['null_keys']})"
        )
    return stats["rows"]


def totals(df: DataFrame) -> tuple[int, Decimal]:
    row = df.agg(F.sum("nb_trips").alias("trips"), F.sum("total_revenue").alias("revenue")).first()
    return int(row["trips"] or 0), row["revenue"] or Decimal(0)


def assert_reconciles(facts: dict[str, DataFrame], silver_trips: int, silver_revenue: Decimal) -> None:
    """Chaque fait partitionne la silver : ses totaux doivent l'égaler exactement."""
    for name, fact in facts.items():
        trips, revenue = totals(fact)
        if (trips, revenue) != (silver_trips, silver_revenue):
            raise GoldValidationError(
                f"{name} does not reconcile with silver: trips {trips} vs {silver_trips}, "
                f"revenue {revenue} vs {silver_revenue}"
            )
