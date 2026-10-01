"""Exécute les étapes du pipeline sans Airflow (debug, exécution hors Docker).

Les étapes et leur ordre sont identiques au DAG. La configuration vient des
variables d'environnement (voir .env.example).

Exemple (datalake sur disque local, sans publication PostgreSQL) :
    DATALAKE_ROOT=./data TRIPS_START_DATE=2023-01-01 TRIPS_END_DATE=2023-01-07 \
        python scripts/run_local.py --skip-publish
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from taxi_pipeline import tasks  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--skip-publish", action="store_true")
    args = parser.parse_args()

    results = {}
    if not args.skip_download:
        results["download"] = [tasks.download_day(day) for day in tasks.list_days()]
    results["bronze"] = tasks.bronze()
    results["silver"] = tasks.silver()
    results["quality_checks"] = tasks.quality_checks()
    results["gold"] = tasks.gold()
    if not args.skip_publish:
        results["publish"] = tasks.publish()
    print(json.dumps(results, indent=2, default=str))


if __name__ == "__main__":
    main()
