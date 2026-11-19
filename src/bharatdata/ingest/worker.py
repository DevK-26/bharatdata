"""Celery app for the ingest worker.

Tonight: the app plus one task that fetches one dataset into the raw zone.
Celery Beat scheduling (from each registry entry's ``refresh_cron``), the
``fetch_runs`` run-log writes, and cleaning/snapshotting are Phase B+ work
(BUILD-SPRINT.md) and are deliberately not wired yet.
"""

from __future__ import annotations

import os
from typing import Any

from celery import Celery

from bharatdata.ingest.fetcher import fetch_dataset
from bharatdata.registry import load_registry

celery_app = Celery(
    "bharatdata",
    broker=os.environ.get("REDIS_URL", "redis://localhost:6379/0"),
)
celery_app.conf.task_ignore_result = True  # run outcomes will go to Postgres fetch_runs


@celery_app.task(name="bharatdata.ingest.fetch_dataset")
def fetch_dataset_task(slug: str) -> dict[str, Any]:
    """Fetch one registry dataset into the raw zone; returns the FetchResult as a dict.

    Loads the whole registry first, so a broken registry file fails the task
    loudly instead of fetching from a half-valid catalogue.
    """
    registry = load_registry()
    if slug not in registry:
        raise KeyError(f"unknown dataset slug {slug!r}; known: {sorted(registry)}")
    return fetch_dataset(registry[slug]).to_dict()
