"""Minimal API app and Celery task. HTTP to data.gov.in is mocked.

The fetch task now writes the fetch_runs log, so its tests need a database:
the throwaway-Postgres fixtures (``pg_dsn``) from conftest provide a real one.
Run-log behaviour in depth: test_runlog.py."""

from __future__ import annotations

import json

import responses
import yaml
from fastapi.testclient import TestClient

from conftest import FIXTURE_RESOURCE_ID, FIXTURE_URL_RE
from bharatdata import __version__
from bharatdata.api.main import app
from bharatdata.ingest.worker import fetch_dataset_task


def test_health_endpoint():
    response = TestClient(app).get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "bharatdata-api", "version": __version__}


def test_openapi_lists_only_health_and_the_admin_run_log():
    paths = TestClient(app).get("/openapi.json").json()["paths"]
    assert sorted(paths) == ["/admin/fetch-runs", "/health"]


def _registry_with_verified_fixture(tmp_path):
    registry = tmp_path / "registry"
    registry.mkdir()
    entry = {
        "slug": "fixture-dataset", "title": "Fixture", "description": "Synthetic.",
        "sector": "testing", "ministry": "Test Ministry", "source_type": "api",
        "resource_id": FIXTURE_RESOURCE_ID, "resource_id_status": "VERIFIED",
        "source_url": "https://data.gov.in/", "format": "json", "refresh_cron": "0 * * * *",
        "expected_schema": {"id": "integer"}, "licence": "GODL-India", "attribution": "Test.",
    }
    (registry / "fixture-dataset.yaml").write_text(yaml.safe_dump(entry), encoding="utf-8")
    return registry


def test_celery_task_fetches_a_registry_dataset(tmp_path, monkeypatch, mocked_http, pg_dsn):
    monkeypatch.setenv("DATABASE_URL", pg_dsn)
    monkeypatch.setenv("BHARATDATA_REGISTRY_DIR", str(_registry_with_verified_fixture(tmp_path)))
    monkeypatch.setenv("BHARATDATA_RAW_DIR", str(tmp_path / "raw"))
    monkeypatch.setenv("DATA_GOV_IN_API_KEY", "test-key")
    body = json.dumps({"total": 2, "records": [{"id": "1"}, {"id": "2"}]}).encode()
    mocked_http.add(responses.GET, FIXTURE_URL_RE, body=body)

    result = fetch_dataset_task.apply(args=["fixture-dataset"]).get()

    assert result["records_fetched"] == 2 and result["complete"] is True
    assert (tmp_path / "raw" / "fixture-dataset").is_dir()
    json.dumps(result)  # the return value is JSON-serialisable


def test_celery_task_rejects_an_unknown_slug(tmp_path, monkeypatch):
    monkeypatch.setenv("BHARATDATA_REGISTRY_DIR", str(_registry_with_verified_fixture(tmp_path)))
    outcome = fetch_dataset_task.apply(args=["no-such-dataset"])
    assert outcome.failed()
    assert "unknown dataset slug" in str(outcome.result)
