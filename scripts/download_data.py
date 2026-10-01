"""Téléchargement reproductible d'une tranche du dataset Chicago Taxi Trips (API SODA).

Même code que l'étape `download` du DAG. Par défaut, la destination est le datalake
configuré (`DATALAKE_ROOT`, MinIO dans Docker) ; `--dest` permet d'écrire ailleurs,
par exemple sur disque local.

Exemples :
    # dans le conteneur Airflow, vers MinIO (période par défaut : .env)
    python scripts/download_data.py
    # en local, un seul jour, 1000 lignes max
    python scripts/download_data.py --start 2023-01-01 --end 2023-01-01 --dest ./data --max-rows-per-day 1000
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import replace
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from taxi_pipeline.config import load_config  # noqa: E402
from taxi_pipeline.ingestion.soda import SodaClient, daterange, ingest_day  # noqa: E402
from taxi_pipeline.utils.storage import get_store  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start", type=date.fromisoformat, help="YYYY-MM-DD inclus (défaut : TRIPS_START_DATE)")
    parser.add_argument("--end", type=date.fromisoformat, help="YYYY-MM-DD inclus (défaut : TRIPS_END_DATE)")
    parser.add_argument("--dest", help="racine du datalake (défaut : DATALAKE_ROOT)")
    parser.add_argument("--page-size", type=int, help="lignes par requête SODA (défaut : SODA_PAGE_SIZE)")
    parser.add_argument("--max-rows-per-day", type=int, help="plafond de lignes par jour (tests rapides)")
    parser.add_argument("--force", action="store_true", help="re-télécharge les jours déjà présents")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config()
    config = config.with_period(args.start or config.start_date, args.end or config.end_date)
    if args.dest:
        config = replace(config, datalake_root=args.dest)
    soda = replace(
        config.soda,
        page_size=args.page_size or config.soda.page_size,
        max_rows_per_day=args.max_rows_per_day or config.soda.max_rows_per_day,
    )

    client, store = SodaClient(soda), get_store(config.datalake_root, config.s3)
    total = 0
    for day in daterange(config.start_date, config.end_date):
        total += ingest_day(day, client, store, config.raw_trips, force=args.force).rows
    print(f"Done: {total} rows downloaded into {config.raw_trips}")


if __name__ == "__main__":
    main()
