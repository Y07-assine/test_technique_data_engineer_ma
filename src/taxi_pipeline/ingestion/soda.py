"""Ingestion depuis l'API SODA du portail open data de Chicago.

Stratégie :
- une requête par jour (filtre sur `trip_start_timestamp`), paginée par `$limit`/`$offset`
  avec un tri stable sur `trip_id` : aucune ligne perdue ni dupliquée entre pages ;
- chaque page est écrite telle quelle (CSV de l'API) dès sa réception : la mémoire
  consommée est bornée par la taille d'une page, quel que soit le volume total ;
- un marqueur `_SUCCESS` est écrit après la dernière page. Un jour marqué est
  considéré complet et n'est pas re-téléchargé ; un jour sans marqueur (ingestion
  interrompue) est purgé puis re-téléchargé entièrement.
"""

from __future__ import annotations

import csv
import io
import time
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Iterator

import requests

from taxi_pipeline.config import SodaConfig
from taxi_pipeline.utils.observability import get_logger
from taxi_pipeline.utils.storage import ObjectStore

logger = get_logger(__name__)

RETRYABLE_STATUS = {429, 500, 502, 503, 504}
SUCCESS_MARKER = "_SUCCESS"


class SodaApiError(RuntimeError):
    pass


@dataclass(frozen=True)
class DayIngestionResult:
    day: str
    rows: int
    pages: int
    skipped: bool


def daterange(start: date, end: date) -> list[date]:
    return [start + timedelta(days=offset) for offset in range((end - start).days + 1)]


def day_where_clause(day: date) -> str:
    next_day = day + timedelta(days=1)
    return (
        f"trip_start_timestamp >= '{day.isoformat()}T00:00:00' "
        f"AND trip_start_timestamp < '{next_day.isoformat()}T00:00:00'"
    )


def count_csv_records(page: str) -> int:
    """Nombre d'enregistrements hors header (gère les champs quotés multi-lignes)."""
    return max(sum(1 for _ in csv.reader(io.StringIO(page))) - 1, 0)


class SodaClient:
    def __init__(self, config: SodaConfig, session: requests.Session | None = None):
        self._config = config
        self._session = session or requests.Session()
        if config.app_token:
            self._session.headers["X-App-Token"] = config.app_token

    def _get(self, params: dict) -> str:
        cfg = self._config
        for attempt in range(1, cfg.max_retries + 1):
            try:
                response = self._session.get(cfg.dataset_url, params=params, timeout=cfg.timeout_seconds)
            # ChunkedEncodingError : connexion coupée en cours de réponse (IncompleteRead).
            except (requests.ConnectionError, requests.Timeout, requests.exceptions.ChunkedEncodingError) as exc:
                error = f"network error: {exc}"
            else:
                if response.status_code == 200:
                    return response.text
                if response.status_code not in RETRYABLE_STATUS:
                    raise SodaApiError(
                        f"SODA returned HTTP {response.status_code} (not retryable): {response.text[:500]}"
                    )
                error = f"HTTP {response.status_code}"
            if attempt == cfg.max_retries:
                raise SodaApiError(f"SODA request failed after {attempt} attempts ({error}), params={params}")
            wait = min(2**attempt, 60)
            logger.warning("SODA request failed (%s), attempt %d/%d, retry in %ds", error, attempt, cfg.max_retries, wait)
            time.sleep(wait)
        raise AssertionError("unreachable")

    def iter_day_pages(self, day: date) -> Iterator[tuple[str, int]]:
        """Itère sur les pages CSV (avec header) des trajets démarrés le jour `day`."""
        page_size = self._config.page_size
        max_rows = self._config.max_rows_per_day
        offset = 0
        while True:
            limit = page_size if max_rows is None else min(page_size, max_rows - offset)
            if limit <= 0:
                return
            page = self._get(
                {"$where": day_where_clause(day), "$order": "trip_id", "$limit": limit, "$offset": offset}
            )
            rows = count_csv_records(page)
            if rows:
                yield page, rows
            if rows < limit:
                return
            offset += rows


def raw_day_prefix(raw_root: str, day: date) -> str:
    return f"{raw_root}/trip_date={day.isoformat()}"


def render_marker(rows: int, pages: int) -> bytes:
    return f"rows={rows}\npages={pages}\n".encode()


def parse_marker(content: str) -> dict[str, int]:
    return {key: int(value) for key, value in (line.split("=", 1) for line in content.split() if "=" in line)}


def ingest_day(
    day: date, client: SodaClient, store: ObjectStore, raw_root: str, force: bool = False
) -> DayIngestionResult:
    prefix = raw_day_prefix(raw_root, day)
    marker = f"{prefix}/{SUCCESS_MARKER}"
    if store.exists(marker) and not force:
        logger.info("day=%s already ingested (marker present), skipped", day)
        return DayIngestionResult(day.isoformat(), rows=0, pages=0, skipped=True)

    store.delete_prefix(prefix)  # repart d'un état propre (ingestion partielle précédente)
    rows = pages = 0
    for page, page_rows in client.iter_day_pages(day):
        store.write_bytes(f"{prefix}/page_{pages:05d}.csv", page.encode("utf-8"))
        rows += page_rows
        pages += 1
    store.write_bytes(marker, render_marker(rows, pages))
    logger.info("day=%s ingested rows=%d pages=%d -> %s", day, rows, pages, prefix)
    return DayIngestionResult(day.isoformat(), rows=rows, pages=pages, skipped=False)
