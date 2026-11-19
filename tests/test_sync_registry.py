"""Registry -> datasets table sync, against a REAL Postgres."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys

import psycopg2
import pytest
import yaml

from conftest import REGISTRY_DIR, REPO_ROOT
from bharatdata import sync_registry
from bharatdata.registry import load_registry


def _rows(conn) -> dict[str, dict]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT slug, id, title, sector, ministry, source_type, resource_id, resource_id_status, "
            "source_url, source_format, refresh_cron, licence, attribution, expected_schema, is_active, updated_at "
            "FROM datasets ORDER BY slug"
        )
        cols = [d.name for d in cur.description]
        return {row[0]: dict(zip(cols, row)) for row in cur.fetchall()}


def test_sync_inserts_all_five_datasets_with_every_field(pg_conn):
    registry = load_registry(REGISTRY_DIR)
    result = sync_registry.sync_registry(pg_conn, registry)

    assert len(registry) == 5
    assert result.inserted == tuple(sorted(registry)) and result.updated == () and result.unchanged == ()
    rows = _rows(pg_conn)
    assert set(rows) == set(registry)
    for slug, entry in registry.items():
        row = rows[slug]
        assert row["title"] == entry.title and row["ministry"] == entry.ministry
        assert row["refresh_cron"] == entry.refresh_cron
        assert row["licence"] == "GODL-India" and row["attribution"] == entry.attribution
        assert row["source_format"] == entry.format
        assert row["expected_schema"] == entry.expected_schema
        # Honesty rule survives the round trip: unverified entries have NO resource id.
        assert (row["resource_id"], row["resource_id_status"]) == (None, "TODO_VERIFY")
        assert row["is_active"] is True


def test_sync_is_idempotent(pg_conn):
    registry = load_registry(REGISTRY_DIR)
    sync_registry.sync_registry(pg_conn, registry)
    before = _rows(pg_conn)

    again = sync_registry.sync_registry(pg_conn, registry)

    assert again.inserted == () and again.updated == () and again.unchanged == tuple(sorted(registry))
    assert _rows(pg_conn) == before  # same ids, same values, and updated_at was not even touched
    assert "0 inserted, 0 updated, 5 unchanged" in str(again)


def test_sync_updates_only_the_entry_that_changed_and_never_deletes(pg_conn, tmp_path):
    directory = tmp_path / "registry"
    shutil.copytree(REGISTRY_DIR, directory)
    sync_registry.sync_registry(pg_conn, load_registry(directory))
    before = _rows(pg_conn)

    path = directory / "pincode-directory.yaml"
    text = path.read_text(encoding="utf-8")
    old_cron = yaml.safe_load(text)["refresh_cron"]
    path.write_text(text.replace(f'"{old_cron}"', '"17 4 * * 3"'), encoding="utf-8")
    result = sync_registry.sync_registry(pg_conn, load_registry(directory))

    assert result.updated == ("pincode-directory",) and len(result.unchanged) == 4
    after = _rows(pg_conn)
    assert after["pincode-directory"]["refresh_cron"] == "17 4 * * 3"
    assert after["pincode-directory"]["id"] == before["pincode-directory"]["id"]
    assert after["pincode-directory"]["updated_at"] > before["pincode-directory"]["updated_at"]
    for slug in set(before) - {"pincode-directory"}:
        assert after[slug] == before[slug]

    (directory / "pincode-directory.yaml").unlink()  # removing a YAML does not delete the catalogue row
    sync_registry.sync_registry(pg_conn, load_registry(directory))
    assert "pincode-directory" in _rows(pg_conn)


def test_a_failed_sync_changes_nothing(pg_conn):
    registry = load_registry(REGISTRY_DIR)
    sync_registry.sync_registry(pg_conn, registry)
    before = _rows(pg_conn)
    broken = dict(registry)
    broken["imd-rainfall"] = registry["imd-rainfall"].model_copy(update={"slug": "BAD SLUG"})  # DB CHECK rejects
    broken["aaa-first"] = registry["imd-rainfall"].model_copy(update={"slug": "aaa-first", "title": "new"})
    with pytest.raises(psycopg2.Error):
        sync_registry.sync_registry(pg_conn, broken)
    assert _rows(pg_conn) == before  # the good "aaa-first" insert was rolled back with the bad one


def test_cli_main_syncs_and_reports(pg_dsn, pg_conn, capsys):
    assert sync_registry.main(["--registry", str(REGISTRY_DIR), "--database-url", pg_dsn]) == 0
    assert "5 inserted, 0 updated, 0 unchanged" in capsys.readouterr().out
    assert sync_registry.main(["--registry", str(REGISTRY_DIR), "--database-url", pg_dsn]) == 0
    assert "0 inserted, 0 updated, 5 unchanged" in capsys.readouterr().out
    assert len(_rows(pg_conn)) == 5


def test_cli_without_a_database_url_exits_2(monkeypatch, capsys):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    assert sync_registry.main([]) == 2
    assert "DATABASE_URL" in capsys.readouterr().err


def test_cli_with_a_broken_registry_exits_2_and_syncs_nothing(pg_dsn, pg_conn, tmp_path, capsys):
    directory = tmp_path / "registry"
    shutil.copytree(REGISTRY_DIR, directory)
    (directory / "imd-rainfall.yaml").write_text("slug: imd-rainfall\n", encoding="utf-8")
    assert sync_registry.main(["--registry", str(directory), "--database-url", pg_dsn]) == 2
    assert "nothing synced" in capsys.readouterr().err
    assert _rows(pg_conn) == {}


def test_python_dash_m_entry_point_works_as_a_real_process(pg_dsn, pg_conn):
    env = {**os.environ, "DATABASE_URL": pg_dsn, "BHARATDATA_REGISTRY_DIR": str(REGISTRY_DIR)}
    done = subprocess.run(
        [sys.executable, "-m", "bharatdata.sync_registry"],
        env=env, capture_output=True, text=True, cwd=REPO_ROOT, timeout=60,
    )
    assert done.returncode == 0, done.stderr
    assert "5 inserted" in done.stdout
    assert len(_rows(pg_conn)) == 5
