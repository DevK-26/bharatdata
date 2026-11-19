"""GET /admin/fetch-runs, against a REAL Postgres; HTTP to data.gov.in mocked."""

from __future__ import annotations

import json

import pytest
import responses
from fastapi.testclient import TestClient

from conftest import FIXTURE_URL_RE, make_entry
from bharatdata.api.main import app
from bharatdata.ingest.fetcher import FetchError
from bharatdata.ingest.worker import run_logged_fetch

TOKEN = "test-admin-token"
AUTH = {"X-Admin-Token": TOKEN}


@pytest.fixture(autouse=True)
def _env(tmp_path, monkeypatch, pg_dsn):
    monkeypatch.setenv("BHARATDATA_RAW_DIR", str(tmp_path / "raw"))
    monkeypatch.setenv("DATA_GOV_IN_API_KEY", "test-key-not-real")
    monkeypatch.setenv("DATABASE_URL", pg_dsn)
    monkeypatch.setenv("BHARATDATA_ADMIN_TOKEN", TOKEN)


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture
def seeded(pg_conn, mocked_http, no_sleep):
    """Three runs, oldest first: ok(alpha), FAILED(beta, HTTP 500), ok(alpha)."""
    alpha, beta = make_entry(slug="alpha-data"), make_entry(slug="beta-data")
    ok = json.dumps({"total": 1, "records": [{"id": "1"}]}).encode()
    mocked_http.add(responses.GET, FIXTURE_URL_RE, body=ok)
    run_logged_fetch(pg_conn, alpha)
    mocked_http.replace(responses.GET, FIXTURE_URL_RE, status=500)
    with pytest.raises(FetchError):
        run_logged_fetch(pg_conn, beta, sleep=no_sleep, max_retries=1)
    mocked_http.replace(responses.GET, FIXTURE_URL_RE, body=ok)
    run_logged_fetch(pg_conn, alpha)


def test_lists_runs_newest_first_including_the_failure_and_its_error(client, seeded):
    response = client.get("/admin/fetch-runs", headers=AUTH)

    assert response.status_code == 200
    body = response.json()
    assert body["count"] == 3
    runs = body["runs"]
    assert [r["dataset_slug"] for r in runs] == ["alpha-data", "beta-data", "alpha-data"]  # newest first
    assert [r["id"] for r in runs] == sorted((r["id"] for r in runs), reverse=True)
    failed = runs[1]
    assert failed["status"] == "failed"
    assert "HTTP 500" in failed["error"] and "2 attempts" in failed["error"]
    assert failed["finished_at"] is not None and failed["records_fetched"] == 0
    assert failed["raw_paths"] == []
    ok = runs[0]
    assert ok["status"] == "succeeded" and ok["error"] is None
    assert ok["records_fetched"] == 1 and ok["pages_fetched"] == 1 and len(ok["raw_paths"]) == 1
    assert set(ok) == {"id", "dataset_slug", "status", "started_at", "finished_at",
                       "records_fetched", "pages_fetched", "raw_paths", "error"}


def test_filters_and_limit(client, seeded):
    failed = client.get("/admin/fetch-runs", params={"status": "failed"}, headers=AUTH).json()
    assert failed["count"] == 1 and failed["runs"][0]["dataset_slug"] == "beta-data"
    alpha = client.get("/admin/fetch-runs", params={"dataset": "alpha-data"}, headers=AUTH).json()
    assert [r["status"] for r in alpha["runs"]] == ["succeeded", "succeeded"]
    assert client.get("/admin/fetch-runs", params={"limit": 2}, headers=AUTH).json()["count"] == 2


@pytest.mark.parametrize("params", [{"limit": 0}, {"limit": 501}, {"status": "weird"}])
def test_bad_query_parameters_are_rejected(client, params):
    assert client.get("/admin/fetch-runs", params=params, headers=AUTH).status_code == 422


def test_empty_log_is_an_empty_list_not_an_error(client):
    assert client.get("/admin/fetch-runs", headers=AUTH).json() == {"count": 0, "runs": []}


def test_requires_the_admin_token(client, seeded):
    assert client.get("/admin/fetch-runs").status_code == 401
    assert client.get("/admin/fetch-runs", headers={"X-Admin-Token": "wrong"}).status_code == 401


def test_admin_api_is_off_when_no_token_is_configured(client, monkeypatch):
    monkeypatch.delenv("BHARATDATA_ADMIN_TOKEN")
    assert client.get("/admin/fetch-runs", headers=AUTH).status_code == 503
    monkeypatch.setenv("BHARATDATA_ADMIN_TOKEN", "")
    assert client.get("/admin/fetch-runs", headers={"X-Admin-Token": ""}).status_code == 503


def test_503_when_the_database_is_not_configured(client, monkeypatch):
    monkeypatch.delenv("DATABASE_URL")
    response = client.get("/admin/fetch-runs", headers=AUTH)
    assert response.status_code == 503 and "DATABASE_URL" in response.json()["detail"]
