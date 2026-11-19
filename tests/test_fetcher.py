"""Fetcher: pagination, retry/backoff, raw-zone storage. ALL HTTP is mocked."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone

import pytest
import requests
import responses

from conftest import FIXTURE_RESOURCE_ID, FIXTURE_URL_RE, make_entry
from bharatdata.ingest.fetcher import (
    FetchError,
    FetchRetriesExhausted,
    fetch_dataset,
    get_api_key,
)

API_KEY = "test-key-NOT-A-REAL-KEY-1234"
TIMESTAMP_NAME = re.compile(r"^\d{8}T\d{6}\.\d{6}Z$")


def serve_dataset(mock, total: int, *, server_cap: int | None = None) -> list[bytes]:
    """Mock the resource endpoint with ``total`` synthetic rows, honouring limit/offset.

    ``server_cap`` emulates the public sample key (returns at most N per page).
    Returns the list of bodies served, in order, to compare against stored files.
    """
    served: list[bytes] = []

    def callback(request):
        query = requests.utils.urlparse(request.url).query
        params = dict(p.split("=", 1) for p in query.split("&"))
        offset, limit = int(params["offset"]), int(params["limit"])
        if server_cap is not None:
            limit = min(limit, server_cap)
        rows = [{"id": str(i), "name": f"row-{i}"} for i in range(offset, min(offset + limit, total))]
        # Unusual spacing on purpose: proves we store the bytes, not a re-dump.
        body = json.dumps(
            {"total": total, "count": len(rows), "limit": str(limit), "offset": str(offset), "records": rows},
            indent=1,
        ).encode()
        served.append(body)
        return 200, {"Content-Type": "application/json"}, body

    mock.add_callback(responses.GET, FIXTURE_URL_RE, callback=callback)
    return served


def run(entry, tmp_path, no_sleep, **kwargs):
    kwargs.setdefault("api_key", API_KEY)
    return fetch_dataset(entry, raw_dir=tmp_path, sleep=no_sleep, **kwargs)


def test_paginates_across_three_pages(entry, tmp_path, mocked_http, no_sleep):
    served = serve_dataset(mocked_http, total=25)

    result = run(entry, tmp_path, no_sleep, page_size=10)

    assert [(p.offset, p.record_count) for p in result.pages] == [(0, 10), (10, 10), (20, 5)]
    assert result.records_fetched == 25
    assert result.total_reported == 25
    assert result.complete is True
    assert len(mocked_http.calls) == 3
    sent = [requests.utils.urlparse(c.request.url).query for c in mocked_http.calls]
    for offset, query in zip((0, 10, 20), sent):
        assert f"offset={offset}" in query and "limit=10" in query and "format=json" in query
        assert f"api-key={API_KEY}" in query
    assert [p.raw_path.read_bytes() for p in result.pages] == served


def test_follows_a_server_that_caps_page_size(entry, tmp_path, mocked_http, no_sleep):
    """The public sample key returns 10 rows whatever limit is asked for."""
    serve_dataset(mocked_http, total=25, server_cap=10)

    result = run(entry, tmp_path, no_sleep, page_size=1000)

    assert [p.offset for p in result.pages] == [0, 10, 20]
    assert result.records_fetched == 25


def test_stops_at_max_records_and_says_incomplete(entry, tmp_path, mocked_http, no_sleep):
    serve_dataset(mocked_http, total=100)

    result = run(entry, tmp_path, no_sleep, page_size=10, max_records=15)

    assert result.records_fetched == 15
    assert result.complete is False
    assert "max_records=15" in result.stop_reason


def test_empty_resource_stores_nothing(entry, tmp_path, mocked_http, no_sleep):
    serve_dataset(mocked_http, total=0)
    result = run(entry, tmp_path, no_sleep)
    assert result.pages == () and result.records_fetched == 0
    assert not (tmp_path / entry.slug).exists()


def test_retries_on_500_then_succeeds(entry, tmp_path, mocked_http, no_sleep):
    mocked_http.add(responses.GET, FIXTURE_URL_RE, status=500, body="boom")
    served = serve_dataset(mocked_http, total=5)

    result = run(entry, tmp_path, no_sleep, page_size=10)

    assert len(mocked_http.calls) == 2  # the 500, then the success, for the same page
    assert no_sleep.waits == [1.0]
    assert result.records_fetched == 5 and len(result.pages) == 1
    assert result.pages[0].raw_path.read_bytes() == served[0]
    # The 500 body was never stored in the raw zone.
    assert len(list((tmp_path / entry.slug).glob("*.json"))) == 2  # payload + sidecar


def test_backoff_doubles_and_honours_retry_after(entry, tmp_path, mocked_http, no_sleep):
    mocked_http.add(responses.GET, FIXTURE_URL_RE, status=503)
    mocked_http.add(responses.GET, FIXTURE_URL_RE, status=429, headers={"Retry-After": "7"})
    mocked_http.add(responses.GET, FIXTURE_URL_RE, status=502)
    serve_dataset(mocked_http, total=3)

    run(entry, tmp_path, no_sleep)

    assert no_sleep.waits == [1.0, 7.0, 4.0]  


def test_backoff_is_capped(entry, tmp_path, mocked_http, no_sleep):
    mocked_http.add(responses.GET, FIXTURE_URL_RE, status=429, headers={"Retry-After": "3600"})
    serve_dataset(mocked_http, total=1)
    run(entry, tmp_path, no_sleep, backoff_max=30.0)
    assert no_sleep.waits == [30.0]


def test_exhausted_retries_raise_a_clear_error(entry, tmp_path, mocked_http, no_sleep):
    mocked_http.add(responses.GET, FIXTURE_URL_RE, status=503)

    with pytest.raises(FetchRetriesExhausted) as excinfo:
        run(entry, tmp_path, no_sleep, max_retries=2)

    message = str(excinfo.value)
    assert "fixture-dataset" in message
    assert "after 3 attempts" in message
    assert "HTTP 503" in message
    assert FIXTURE_RESOURCE_ID in message
    assert API_KEY not in message  # the key never leaks into errors
    assert len(mocked_http.calls) == 3
    assert no_sleep.waits == [1.0, 2.0]  # no pointless wait after the final attempt
    assert not (tmp_path / entry.slug).exists()
    assert isinstance(excinfo.value, FetchError)


def test_network_errors_are_retried_and_redacted(entry, tmp_path, mocked_http, no_sleep):
    leaky = requests.ConnectionError(f"HTTPSConnectionPool: max retries exceeded ...?api-key={API_KEY}")
    mocked_http.add(responses.GET, FIXTURE_URL_RE, body=leaky)

    with pytest.raises(FetchRetriesExhausted) as excinfo:
        run(entry, tmp_path, no_sleep, max_retries=1)

    assert "ConnectionError" in str(excinfo.value)
    assert API_KEY not in str(excinfo.value)
    assert "api-key=***" in str(excinfo.value)
    assert len(mocked_http.calls) == 2


@pytest.mark.parametrize("status", [400, 401, 403, 404])
def test_client_errors_are_not_retried(entry, tmp_path, mocked_http, no_sleep, status):
    mocked_http.add(responses.GET, FIXTURE_URL_RE, status=status)
    with pytest.raises(FetchError, match=f"HTTP {status}; not retryable") as excinfo:
        run(entry, tmp_path, no_sleep)
    assert not isinstance(excinfo.value, FetchRetriesExhausted)
    assert len(mocked_http.calls) == 1 and no_sleep.waits == []


def test_raw_file_and_sidecar_are_written_with_correct_sha256(entry, tmp_path, mocked_http, no_sleep):
    served = serve_dataset(mocked_http, total=7)
    before = datetime.now(timezone.utc)

    result = run(entry, tmp_path, no_sleep, page_size=10)

    (page,) = result.pages
    directory = tmp_path / entry.slug
    assert page.raw_path.parent == directory
    assert TIMESTAMP_NAME.match(page.raw_path.stem)
    assert page.raw_path.read_bytes() == served[0]  # byte-for-byte

    meta = json.loads(page.meta_path.read_text(encoding="utf-8"))
    assert page.meta_path.name == f"{page.raw_path.stem}.meta.json"
    assert meta["sha256"] == hashlib.sha256(served[0]).hexdigest() == page.sha256
    assert meta["record_count"] == 7
    assert meta["size_bytes"] == len(served[0])
    assert meta["slug"] == entry.slug and meta["resource_id"] == FIXTURE_RESOURCE_ID
    assert meta["source_url"].startswith(f"https://api.data.gov.in/resource/{FIXTURE_RESOURCE_ID}?")
    assert "offset=0" in meta["source_url"] and "limit=10" in meta["source_url"]
    assert meta["payload_file"] == page.raw_path.name
    assert datetime.fromisoformat(meta["fetched_at"]) >= before
    assert meta["run_id"] == result.run_id


def test_api_key_is_never_persisted(entry, tmp_path, mocked_http, no_sleep):
    serve_dataset(mocked_http, total=25)
    run(entry, tmp_path, no_sleep, page_size=10)
    for path in tmp_path.rglob("*"):
        assert API_KEY not in path.name
        if path.is_file():
            assert API_KEY.encode() not in path.read_bytes(), path


def test_all_pages_of_a_run_share_a_run_id(entry, tmp_path, mocked_http, no_sleep):
    serve_dataset(mocked_http, total=25)
    result = run(entry, tmp_path, no_sleep, page_size=10)
    run_ids = {json.loads(p.meta_path.read_text())["run_id"] for p in result.pages}
    assert run_ids == {result.run_id}
    assert len({p.raw_path.name for p in result.pages}) == 3


def test_existing_raw_files_are_never_overwritten(entry, tmp_path, mocked_http, no_sleep):
    serve_dataset(mocked_http, total=3)
    frozen = datetime(2026, 11, 16, 12, 0, 0, 0, tzinfo=timezone.utc)

    first = run(entry, tmp_path, no_sleep, now=lambda: frozen)
    original = first.pages[0].raw_path.read_bytes()
    second = run(entry, tmp_path, no_sleep, now=lambda: frozen)  # identical clock reading

    assert first.pages[0].raw_path != second.pages[0].raw_path
    assert first.pages[0].raw_path.read_bytes() == original
    assert len(list((tmp_path / entry.slug).glob("*.meta.json"))) == 2


def test_failure_on_a_later_page_keeps_earlier_pages_and_reports_them(entry, tmp_path, mocked_http, no_sleep):
    calls = {"n": 0}

    def callback(request):
        calls["n"] += 1
        if calls["n"] == 1:
            rows = [{"id": str(i)} for i in range(10)]
            return 200, {}, json.dumps({"total": 25, "records": rows}).encode()
        return 500, {}, b"down"

    mocked_http.add_callback(responses.GET, FIXTURE_URL_RE, callback=callback)

    with pytest.raises(FetchRetriesExhausted) as excinfo:
        run(entry, tmp_path, no_sleep, page_size=10, max_retries=1)

    assert len(excinfo.value.pages) == 1
    assert excinfo.value.pages[0].raw_path.exists()  # raw zone keeps what was fetched
    assert "offset=10" in str(excinfo.value)


@pytest.mark.parametrize(
    "body",
    [b"<html>maintenance</html>", b'{"status": "error", "message": "bad key"}', b"[1, 2, 3]"],
)
def test_unexpected_200_responses_fail_and_are_not_stored(entry, tmp_path, mocked_http, no_sleep, body):
    mocked_http.add(responses.GET, FIXTURE_URL_RE, status=200, body=body)
    with pytest.raises(FetchError, match="fixture-dataset"):
        run(entry, tmp_path, no_sleep)
    assert not (tmp_path / entry.slug).exists()


def test_refuses_an_unverified_dataset_without_any_http(tmp_path, mocked_http, no_sleep):
    unverified = make_entry(resource_id=None, resource_id_status="TODO_VERIFY")
    with pytest.raises(FetchError, match="not fetchable"):
        run(unverified, tmp_path, no_sleep)
    assert len(mocked_http.calls) == 0


def test_api_key_comes_from_the_environment_only(entry, tmp_path, mocked_http, no_sleep, monkeypatch):
    serve_dataset(mocked_http, total=2)
    monkeypatch.delenv("DATA_GOV_IN_API_KEY", raising=False)
    with pytest.raises(FetchError, match="DATA_GOV_IN_API_KEY is not set"):
        fetch_dataset(entry, raw_dir=tmp_path, sleep=no_sleep)
    assert len(mocked_http.calls) == 0

    monkeypatch.setenv("DATA_GOV_IN_API_KEY", "env-key-xyz")
    fetch_dataset(entry, raw_dir=tmp_path, sleep=no_sleep)
    assert "api-key=env-key-xyz" in mocked_http.calls[0].request.url


def test_blank_api_key_is_rejected():
    assert get_api_key({"DATA_GOV_IN_API_KEY": "abc "}) == "abc"
    with pytest.raises(FetchError):
        get_api_key({"DATA_GOV_IN_API_KEY": "   "})


def test_raw_dir_defaults_to_environment(entry, tmp_path, mocked_http, no_sleep, monkeypatch):
    serve_dataset(mocked_http, total=2)
    monkeypatch.setenv("BHARATDATA_RAW_DIR", str(tmp_path / "from-env"))
    result = fetch_dataset(entry, api_key=API_KEY, sleep=no_sleep)
    assert result.pages[0].raw_path.parent == tmp_path / "from-env" / entry.slug
