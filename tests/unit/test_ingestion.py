from __future__ import annotations

from datetime import date

import pytest
import requests

from taxi_pipeline.config import SodaConfig
from taxi_pipeline.ingestion import soda
from taxi_pipeline.ingestion.soda import (
    SodaApiError,
    SodaClient,
    count_csv_records,
    daterange,
    day_where_clause,
    ingest_day,
    parse_marker,
    raw_day_prefix,
)
from taxi_pipeline.utils.storage import LocalStore

HEADER = '"trip_id","fare"\n'


class FakeResponse:
    def __init__(self, status_code: int, text: str = ""):
        self.status_code = status_code
        self.text = text


class FakeSession:
    """Rejoue une liste de réponses et enregistre les paramètres reçus."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []
        self.headers = {}

    def get(self, url, params, timeout):
        self.calls.append(dict(params))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def page(n_rows: int, start: int = 0) -> str:
    return HEADER + "".join(f'"t{i}","1.0"\n' for i in range(start, start + n_rows))


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(soda.time, "sleep", lambda _: None)


def test_daterange_is_inclusive():
    assert daterange(date(2023, 1, 30), date(2023, 2, 1)) == [date(2023, 1, 30), date(2023, 1, 31), date(2023, 2, 1)]


def test_day_where_clause_covers_exactly_one_day():
    assert day_where_clause(date(2023, 1, 31)) == (
        "trip_start_timestamp >= '2023-01-31T00:00:00' AND trip_start_timestamp < '2023-02-01T00:00:00'"
    )


def test_count_csv_records_handles_quoted_newlines():
    assert count_csv_records(HEADER + '"a","multi\nline"\n"b","2"\n') == 2
    assert count_csv_records(HEADER) == 0


def test_pagination_stops_on_short_page():
    session = FakeSession([FakeResponse(200, page(2)), FakeResponse(200, page(1, start=2))])
    client = SodaClient(SodaConfig(page_size=2), session=session)

    pages = list(client.iter_day_pages(date(2023, 1, 1)))

    assert [rows for _, rows in pages] == [2, 1]
    assert [c["$offset"] for c in session.calls] == [0, 2]
    assert all(c["$order"] == "trip_id" for c in session.calls)


def test_max_rows_per_day_caps_download():
    session = FakeSession([FakeResponse(200, page(2)), FakeResponse(200, page(1))])
    client = SodaClient(SodaConfig(page_size=2, max_rows_per_day=3), session=session)

    assert sum(rows for _, rows in client.iter_day_pages(date(2023, 1, 1))) == 3
    assert session.calls[1]["$limit"] == 1


def test_retryable_errors_are_retried():
    truncated = requests.exceptions.ChunkedEncodingError("Connection broken: IncompleteRead(7275 bytes read)")
    session = FakeSession([
        FakeResponse(503),
        requests.ConnectionError("boom"),
        requests.Timeout("read timed out"),
        truncated,
        FakeResponse(200, page(1)),
    ])
    client = SodaClient(SodaConfig(page_size=10, max_retries=5), session=session)

    assert sum(rows for _, rows in client.iter_day_pages(date(2023, 1, 1))) == 1
    assert len(session.calls) == 5


def test_non_retryable_error_fails_immediately():
    session = FakeSession([FakeResponse(400, "bad query")])
    client = SodaClient(SodaConfig(max_retries=5), session=session)

    with pytest.raises(SodaApiError, match="HTTP 400"):
        list(client.iter_day_pages(date(2023, 1, 1)))
    assert len(session.calls) == 1


def test_retries_exhausted_raise():
    session = FakeSession([FakeResponse(429)] * 2)
    client = SodaClient(SodaConfig(max_retries=2), session=session)

    with pytest.raises(SodaApiError, match="after 2 attempts"):
        list(client.iter_day_pages(date(2023, 1, 1)))


def test_ingest_day_is_idempotent(tmp_path):
    store, raw_root, day = LocalStore(), (tmp_path / "raw").as_posix(), date(2023, 1, 1)
    first = SodaClient(SodaConfig(page_size=10), session=FakeSession([FakeResponse(200, page(3))]))

    result = ingest_day(day, first, store, raw_root)
    assert (result.rows, result.pages, result.skipped) == (3, 1, False)
    marker = store.read_bytes(f"{raw_day_prefix(raw_root, day)}/_SUCCESS").decode()
    assert parse_marker(marker) == {"rows": 3, "pages": 1}

    # Deuxième passage : aucun appel API, le jour est déjà complet.
    second_session = FakeSession([])
    again = ingest_day(day, SodaClient(SodaConfig(), session=second_session), store, raw_root)
    assert again.skipped and second_session.calls == []


def test_partial_ingestion_is_cleaned_before_retry(tmp_path):
    store, raw_root, day = LocalStore(), (tmp_path / "raw").as_posix(), date(2023, 1, 1)
    prefix = raw_day_prefix(raw_root, day)
    store.write_bytes(f"{prefix}/page_00007.csv", b"stale page from an interrupted run")

    client = SodaClient(SodaConfig(page_size=10), session=FakeSession([FakeResponse(200, page(2))]))
    ingest_day(day, client, store, raw_root)

    assert not store.exists(f"{prefix}/page_00007.csv")
    assert store.exists(f"{prefix}/page_00000.csv")
