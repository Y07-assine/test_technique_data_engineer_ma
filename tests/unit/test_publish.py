from __future__ import annotations

from taxi_pipeline.gold.tables import GOLD_TABLES
from taxi_pipeline.publish.postgres import swap_statements


def test_swap_runs_in_dependency_safe_order():
    """Tout est supprimé, puis renommé, puis contraint : aucun ordre de tables ne peut bloquer."""
    kinds = [
        "drop" if s.startswith("DROP") else "rename" if "RENAME" in s else "pk" if "PRIMARY KEY" in s else "fk"
        for s in swap_statements(GOLD_TABLES)
    ]
    assert kinds == sorted(kinds, key=["drop", "rename", "pk", "fk"].index)
    assert kinds.count("drop") == kinds.count("rename") == kinds.count("pk") == len(GOLD_TABLES)


def test_foreign_keys_reference_published_dimensions():
    fks = [s for s in swap_statements(GOLD_TABLES) if "FOREIGN KEY" in s]
    assert 'ALTER TABLE public."fact_zone_metrics" ADD FOREIGN KEY ("zone_key") ' \
           'REFERENCES public."dim_zone" ("zone_key")' in fks
    assert len(fks) == 7  # 4 faits -> dim_date + 3 autres dimensions


def test_drops_cascade_to_release_foreign_keys():
    drops = [s for s in swap_statements(GOLD_TABLES) if s.startswith("DROP")]
    assert all(s.endswith("CASCADE") for s in drops)
    assert 'DROP TABLE IF EXISTS public."gold_daily_metrics" CASCADE' in drops
