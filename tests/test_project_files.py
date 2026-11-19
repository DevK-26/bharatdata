"""Guards on the non-Python deliverables: schema, compose, env, dependency lists."""

from __future__ import annotations

import re
import tomllib

import pytest
import yaml

from conftest import REPO_ROOT


def _requirement_names(lines):
    names = set()
    for line in lines:
        line = line.strip()
        if line and not line.startswith("#"):
            names.add(line)
    return names


def test_requirements_txt_matches_pyproject():
    pyproject = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    declared = set(pyproject["dependencies"]) | set(pyproject["optional-dependencies"]["dev"])
    listed = _requirement_names((REPO_ROOT / "requirements.txt").read_text(encoding="utf-8").splitlines())
    assert listed == declared


def test_compose_has_exactly_the_five_services_and_is_wired_consistently():
    compose = yaml.safe_load((REPO_ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
    services = compose["services"]
    assert set(services) == {"caddy", "api", "ingest", "postgres", "redis"}

    # Caddy is the only public door.
    assert [name for name, svc in services.items() if "ports" in svc] == ["caddy"]
    # depends_on targets exist; named volumes are declared.
    declared_volumes = set(compose["volumes"])
    for svc in services.values():
        depends = svc.get("depends_on", [])
        assert set(depends) <= set(services)
        for mount in svc.get("volumes", []):
            source = mount.split(":")[0]
            if not source.startswith("."):
                assert source in declared_volumes, mount
    assert "bharatdata.api.main:app" in " ".join(services["api"]["command"])
    assert "bharatdata.ingest.worker" in " ".join(services["ingest"]["command"])
    # The schema is mounted for first boot, and the file it mounts exists.
    assert any(m.startswith("./db/schema.sql:") for m in services["postgres"]["volumes"])
    assert (REPO_ROOT / "db" / "schema.sql").is_file()
    assert (REPO_ROOT / "deploy" / "caddy" / "Caddyfile").is_file()


def test_caddy_routes_api_to_the_api_service():
    caddyfile = (REPO_ROOT / "deploy" / "caddy" / "Caddyfile").read_text(encoding="utf-8")
    assert "reverse_proxy api:8000" in caddyfile
    assert "{$DOMAIN}" in caddyfile


def test_env_example_has_a_blank_api_key_and_no_secrets():
    env = dict(
        line.split("=", 1)
        for line in (REPO_ROOT / ".env.example").read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#")
    )
    assert env["DATA_GOV_IN_API_KEY"] == ""
    assert {"DOMAIN", "POSTGRES_PASSWORD", "DATABASE_URL", "REDIS_URL", "BHARATDATA_RAW_DIR"} <= set(env)


def test_gitignore_protects_secrets_and_raw_contents():
    lines = (REPO_ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()
    assert ".env" in lines
    assert "data/raw/*" in lines and "!data/raw/.gitkeep" in lines


def test_no_api_key_literal_in_source():
    """The key comes from the environment; no source file may assign one."""
    pattern = re.compile(r"""api[_-]?key\s*=\s*["'][^"']{8,}["']""", re.IGNORECASE)
    for path in (REPO_ROOT / "src").rglob("*.py"):
        assert not pattern.search(path.read_text(encoding="utf-8")), path


# --- db/schema.sql ----------------------------------------------------------

SCHEMA = (REPO_ROOT / "db" / "schema.sql").read_text(encoding="utf-8")


def _table_body(name: str) -> str:
    match = re.search(rf"CREATE TABLE {name} \((.*?)\n\);", SCHEMA, re.DOTALL)
    assert match, f"table {name} missing from schema.sql"
    return match.group(1)


@pytest.mark.parametrize(
    ("table", "columns"),
    [
        ("datasets", ["slug", "title", "ministry", "sector", "source_type", "resource_id", "source_url",
                      "refresh_cron", "licence", "attribution", "expected_schema"]),
        ("snapshots", ["dataset_id", "content_hash", "row_count", "created_at", "parquet_path"]),
        ("fetch_runs", ["dataset_id", "started_at", "finished_at", "status", "records_fetched", "error"]),
        ("api_keys", ["key_hash", "email", "daily_limit", "revoked"]),
        ("users", ["email", "created_at"]),
    ],
)
def test_schema_tables_have_the_required_columns(table, columns):
    body = _table_body(table)
    for column in columns:
        assert re.search(rf"^\s+{column}\s", body, re.MULTILINE), f"{table}.{column}"


def test_schema_foreign_keys_and_indexes():
    assert "dataset_id          bigint      NOT NULL REFERENCES datasets (id)" in _table_body("fetch_runs")
    assert "REFERENCES datasets (id)" in _table_body("snapshots")
    for index in ("datasets_search_idx", "fetch_runs_dataset_started_idx",
                  "snapshots_dataset_created_idx", "users_email_lower_key"):
        assert f"INDEX {index}" in SCHEMA


def test_schema_parses_with_the_real_postgres_parser():
    """Syntax only. Behaviour was checked on a real server by scripts/verify_schema_pg.py."""
    import pglast

    statements = pglast.parse_sql(SCHEMA)
    assert len(statements) > 20
