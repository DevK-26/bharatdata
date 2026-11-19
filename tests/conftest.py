"""Shared fixtures. No test in this suite may touch the network: every HTTP
call goes through ``responses`` (which fails on any unmocked URL)."""

from __future__ import annotations

import re
from pathlib import Path

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
