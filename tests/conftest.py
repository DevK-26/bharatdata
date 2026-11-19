"""Shared fixtures. No test in this suite may touch the network: every HTTP
call goes through ``responses`` (which fails on any unmocked URL)."""

from __future__ import annotations

import itertools
import re
import shutil
import tempfile
from pathlib import Path

import psycopg2
import pytest
import responses

from bharatdata.registry import DatasetEntry

REPO_ROOT = Path(__file__).resolve().parent.parent
REGISTRY_DIR = REPO_ROOT / "registry"

# Obviously fake id used ONLY by tests/demo; never written to registry/.
FIXTURE_RESOURCE_ID = "00000000-0000-0000-0000-000000000000"
FIXTURE_URL_RE = re.compile(rf"^https://api\.data\.gov\.in/resource/{FIXTURE_RESOURCE_ID}(\?.*)?$")


def make_entry(**overrides) -> DatasetEntry:
    """A valid, fetchable entry for a synthetic fixture dataset."""
    fields = dict(
        slug="fixture-dataset",
        title="Fixture Dataset",
        description="Synthetic dataset used only by tests.",
        sector="testing",
        ministry="Test Ministry",
        source_type="api",
        resource_id=FIXTURE_RESOURCE_ID,
        resource_id_status="VERIFIED",
        source_url="https://data.gov.in/",
        format="json",
        refresh_cron="0 * * * *",
        expected_schema={"id": "integer", "name": "string"},
        licence="GODL-India",
        attribution="Test attribution.",
    )
    fields.update(overrides)
    return DatasetEntry.model_validate(fields)


@pytest.fixture
def entry() -> DatasetEntry:
    return make_entry()


@pytest.fixture
def mocked_http():
    """Active `responses` mock; any request not registered raises ConnectionError."""
    with responses.RequestsMock(assert_all_requests_are_fired=False) as mock:
        yield mock


class RecordingSleep:
    """Stands in for time.sleep: records each requested wait, never actually waits."""

    def __init__(self) -> None:
        self.waits: list[float] = []

    def __call__(self, seconds: float) -> None:
        self.waits.append(seconds)


@pytest.fixture
def no_sleep() -> RecordingSleep:
    return RecordingSleep()


# --- real throwaway Postgres -------------------------------------------------
# Same approach as scripts/verify_schema_pg.py: `pgserver` bundles real Postgres
# binaries and runs a private server in a temp dir. No mocked database anywhere.
# NOTE: pgserver drops privileges to an unprivileged user when run as root, which
# cannot read libraries inside a root-only checkout. In such a sandbox install it
# somewhere world-readable and prepend it:  PYTHONPATH=/tmp/bd-pgserver pytest
# (see README "Running the tests").

_db_counter = itertools.count(1)


@pytest.fixture(scope="session")
def pg_server():
    import pgserver

    # pgserver's cleanup() only stops the server; the ~50 MB data dir is ours to remove.
    pgdata = tempfile.mkdtemp(prefix="bdpg-tests-")
    server = pgserver.get_server(pgdata)
    try:
        yield server
    finally:
        server.cleanup()
        shutil.rmtree(pgdata, ignore_errors=True)


@pytest.fixture
def pg_dsn(pg_server) -> str:
    """DSN of a brand-new database with db/schema.sql loaded (one per test)."""
    name = f"bd_test_{next(_db_counter)}"
    admin = psycopg2.connect(pg_server.get_uri())
    admin.autocommit = True
    try:
        admin.cursor().execute(f"CREATE DATABASE {name}")
        dsn = pg_server.get_uri(database=name)
        loader = psycopg2.connect(dsn)
        try:
            with loader, loader.cursor() as cur:  # `with conn` commits; it does not close
                cur.execute((REPO_ROOT / "db" / "schema.sql").read_text(encoding="utf-8"))
        finally:
            loader.close()
        yield dsn
        admin.cursor().execute(f"DROP DATABASE {name} WITH (FORCE)")
    finally:
        admin.close()


@pytest.fixture
def pg_conn(pg_dsn):
    conn = psycopg2.connect(pg_dsn)
    yield conn
    conn.close()
