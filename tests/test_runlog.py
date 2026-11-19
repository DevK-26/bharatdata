"""fetch_runs run log, against a REAL Postgres; HTTP mocked."""

from __future__ import annotations

import functools
import json
from pathlib import Path

import psycopg2
import pytest
import responses

from conftest import FIXTURE_RESOURCE_ID, FIXTURE_URL_RE, make_entry
from bharatdata.ingest import runlog, worker
from bharatdata.ingest.fetcher import FetchError, FetchRetriesExhausted, fetch_dataset
from bharatdata.ingest.worker import fetch_dataset_task, run_logged_fetch
from bharatdata.registry import load_registry

from test_api_and_worker import _registry_with_verified_fixture


def _body(records: list[dict], total: int) -> bytes:
    return json.dumps({"total": total, "records": records}).encode()


def _runs(conn) -> list[dict]:
    return runlog.list_runs(conn, limit=100)


@pytest.fixture(autouse=True)
def _env(tmp_path, monkeypatch):
    monkeypatch.setenv("BHARATDATA_RAW_DIR", str(tmp_path / "raw"))
    monkeypatch.setenv("DATA_GOV_IN_API_KEY", "test-key-not-real")


@pytest.fixture
def fast_fetch(monkeypatch, no_sleep):
    """The task's fetch_dataset, but with the backoff sleeps recorded instead of slept."""
    monkeypatch.setattr(worker, "fetch_dataset", functools.partial(fetch_dataset, sleep=no_sleep))
    return no_sleep


def test_successful_fetch_leaves_a_succeeded_row_with_paths_and_counts(pg_conn, mocked_http, entry):
    mocked_http.add(responses.GET, FIXTURE_URL_RE, body=_body([{"id": "1"}, {"id": "2"}], total=3))
    mocked_http.add(responses.GET, FIXTURE_URL_RE, body=_body([{"id": "3"}], total=3))

    result = run_logged_fetch(pg_conn, entry, page_size=2)

    (run,) = _runs(pg_conn)
    assert run["status"] == "succeeded"
    assert run["dataset_slug"] == "fixture-dataset"
    assert run["records_fetched"] == 3 and run["pages_fetched"] == 2
    assert run["error"] is None and run["finished_at"] >= run["started_at"]
    assert run["raw_paths"] == [page["raw_path"] for page in result["pages"]]
    assert result["fetch_run_id"] == run["id"]
    for path in run["raw_paths"]:  # the paths in the log are real files in the raw zone
        assert Path(path).is_file()


def test_failing_fetch_after_retries_leaves_a_failed_row_with_the_error_and_reraises(
    pg_conn, mocked_http, entry, no_sleep
):
    mocked_http.add(responses.GET, FIXTURE_URL_RE, status=500, body="boom")

    with pytest.raises(FetchRetriesExhausted):
        run_logged_fetch(pg_conn, entry, sleep=no_sleep, max_retries=3)

    assert len(mocked_http.calls) == 4  # first attempt + 3 retries: retries really were exhausted
    assert len(no_sleep.waits) == 3
    (run,) = _runs(pg_conn)
    assert run["status"] == "failed"
    assert "FetchRetriesExhausted" in run["error"]
    assert "HTTP 500" in run["error"] and "giving up" in run["error"] and "4 attempts" in run["error"]
    assert run["finished_at"] is not None
    assert run["records_fetched"] == 0 and run["raw_paths"] == []
    assert "test-key-not-real" not in run["error"]  # the API key never reaches the log


def test_failure_on_a_later_page_keeps_the_pages_already_stored(pg_conn, mocked_http, entry, no_sleep):
    mocked_http.add(responses.GET, FIXTURE_URL_RE, body=_body([{"id": "1"}, {"id": "2"}], total=4))
    mocked_http.add(responses.GET, FIXTURE_URL_RE, status=503)

    with pytest.raises(FetchError):
        run_logged_fetch(pg_conn, entry, page_size=2, sleep=no_sleep, max_retries=1)

    (run,) = _runs(pg_conn)
    assert run["status"] == "failed" and "HTTP 503" in run["error"]
    assert run["records_fetched"] == 2 and len(run["raw_paths"]) == 1
    assert Path(run["raw_paths"][0]).is_file()  # raw zone is immutable: the page stays, and is logged


def test_unfetchable_dataset_is_a_visible_failed_row_not_silence(pg_conn):
    unverified = make_entry(resource_id=None, resource_id_status="TODO_VERIFY")
    with pytest.raises(FetchError, match="not fetchable"):
        run_logged_fetch(pg_conn, unverified)
    (run,) = _runs(pg_conn)
    assert run["status"] == "failed" and "not fetchable" in run["error"]


def test_non_fetch_errors_are_logged_too_and_reraised(pg_conn, entry, monkeypatch):
    def explode(*a, **k):
        raise RuntimeError("disk full")

    monkeypatch.setattr(worker, "fetch_dataset", explode)
    with pytest.raises(RuntimeError, match="disk full"):
        run_logged_fetch(pg_conn, entry)
    (run,) = _runs(pg_conn)
    assert run["status"] == "failed" and run["error"] == "RuntimeError: disk full"


def test_the_row_is_running_and_visible_to_others_while_the_fetch_is_in_flight(pg_dsn, pg_conn, entry, monkeypatch):
    seen = {}

    def spy(entry_, **kwargs):
        other = psycopg2.connect(pg_dsn)  # a different session: only committed rows are visible
        try:
            seen["rows"] = runlog.list_runs(other)
        finally:
            other.close()
        raise FetchError("stop here")

    monkeypatch.setattr(worker, "fetch_dataset", spy)
    with pytest.raises(FetchError):
        run_logged_fetch(pg_conn, entry)

    (during,) = seen["rows"]
    assert during["status"] == "running" and during["finished_at"] is None and during["error"] is None
    assert _runs(pg_conn)[0]["status"] == "failed"


def test_a_finished_run_cannot_be_overwritten(pg_conn, entry, mocked_http):
    mocked_http.add(responses.GET, FIXTURE_URL_RE, body=_body([{"id": "1"}], total=1))
    result = run_logged_fetch(pg_conn, entry)
    with pytest.raises(RuntimeError, match="not in the 'running' state"):
        runlog.finish_failed(pg_conn, result["fetch_run_id"], "late failure")
    assert _runs(pg_conn)[0]["status"] == "succeeded"


def test_failed_status_without_error_text_is_still_stored_with_a_placeholder(pg_conn, entry):
    with pg_conn.cursor() as cur:
        from bharatdata.sync_registry import upsert_entry
        dataset_id, _ = upsert_entry(cur, entry)
    pg_conn.commit()
    run_id = runlog.start_run(pg_conn, dataset_id)
    runlog.finish_failed(pg_conn, run_id, "   ")
    assert "unknown error" in _runs(pg_conn)[0]["error"]


def test_runs_are_listed_newest_first_and_filterable(pg_conn, mocked_http, entry, no_sleep):
    mocked_http.add(responses.GET, FIXTURE_URL_RE, body=_body([{"id": "1"}], total=1))
    run_logged_fetch(pg_conn, entry)                       # 1: succeeded
    mocked_http.reset()
    mocked_http.add(responses.GET, FIXTURE_URL_RE, status=500)
    with pytest.raises(FetchError):
        run_logged_fetch(pg_conn, entry, sleep=no_sleep, max_retries=0)  # 2: failed
    runs = _runs(pg_conn)
    assert [r["status"] for r in runs] == ["failed", "succeeded"]
    assert [r["id"] for r in runs] == sorted((r["id"] for r in runs), reverse=True)
    assert [r["status"] for r in runlog.list_runs(pg_conn, status="failed")] == ["failed"]
    assert runlog.list_runs(pg_conn, slug="nope") == []
    assert len(runlog.list_runs(pg_conn, limit=1)) == 1


# --- through the actual Celery task ------------------------------------------


def test_celery_task_success_end_to_end(tmp_path, monkeypatch, mocked_http, pg_dsn, pg_conn, fast_fetch):
    monkeypatch.setenv("BHARATDATA_REGISTRY_DIR", str(_registry_with_verified_fixture(tmp_path)))
    monkeypatch.setenv("DATABASE_URL", pg_dsn)
    mocked_http.add(responses.GET, FIXTURE_URL_RE, body=_body([{"id": "1"}, {"id": "2"}], total=2))

    outcome = fetch_dataset_task.apply(args=["fixture-dataset"])

    assert outcome.successful(), outcome.result
    (run,) = _runs(pg_conn)
    assert (run["status"], run["records_fetched"], run["pages_fetched"]) == ("succeeded", 2, 1)
    assert outcome.result["fetch_run_id"] == run["id"]
    assert Path(run["raw_paths"][0]).parent == tmp_path / "raw" / "fixture-dataset"


def test_celery_task_failure_is_logged_and_the_task_state_is_failure(
    tmp_path, monkeypatch, mocked_http, pg_dsn, pg_conn, fast_fetch
):
    monkeypatch.setenv("BHARATDATA_REGISTRY_DIR", str(_registry_with_verified_fixture(tmp_path)))
    monkeypatch.setenv("DATABASE_URL", pg_dsn)
    mocked_http.add(responses.GET, FIXTURE_URL_RE, status=500)

    outcome = fetch_dataset_task.apply(args=["fixture-dataset"])

    assert outcome.failed()  # Celery convention: the exception is re-raised, state is FAILURE
    assert isinstance(outcome.result, FetchRetriesExhausted)
    assert len(mocked_http.calls) == 6  # default policy: 1 attempt + 5 retries, then give up
    (run,) = _runs(pg_conn)
    assert run["status"] == "failed" and "HTTP 500" in run["error"] and "6 attempts" in run["error"]
    assert FIXTURE_RESOURCE_ID in run["error"]


def test_celery_task_without_a_database_fails_loudly(tmp_path, monkeypatch, mocked_http):
    monkeypatch.setenv("BHARATDATA_REGISTRY_DIR", str(_registry_with_verified_fixture(tmp_path)))
    monkeypatch.delenv("DATABASE_URL", raising=False)
    outcome = fetch_dataset_task.apply(args=["fixture-dataset"])
    assert outcome.failed() and "DATABASE_URL" in str(outcome.result)
    assert len(mocked_http.calls) == 0  # no fetch happens without a run log to record it in


def test_every_real_registry_dataset_fails_visibly_today_because_none_is_verified(
    pg_dsn, pg_conn, monkeypatch
):
    """All five real entries are TODO_VERIFY: a scheduled run must say so in the log, not skip silently."""
    monkeypatch.setenv("DATABASE_URL", pg_dsn)
    registry = load_registry()
    for slug in registry:
        assert fetch_dataset_task.apply(args=[slug]).failed()
    runs = _runs(pg_conn)
    assert sorted(r["dataset_slug"] for r in runs) == sorted(registry)
    assert all(r["status"] == "failed" and "not fetchable" in r["error"] for r in runs)
