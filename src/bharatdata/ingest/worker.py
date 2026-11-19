"""Celery app for the ingest worker and the Beat scheduler.

Scheduled run, end to end::

    beat fires fetch-<slug> (cron from registry/<slug>.yaml)
      -> fetch_dataset_task(slug)
      -> fetch_runs row 'running'            (committed before any HTTP)
      -> fetcher writes the immutable raw zone
      -> row updated: 'succeeded' with raw_paths + record counts,
         or 'failed' with the error text, after which the exception is re-raised
         so Celery also marks the task FAILURE (never swallowed).

Cleaning and snapshotting are Phase B2/B3 and are not wired yet.
"""

from __future__ import annotations

import os
from typing import Any

from celery import Celery

from bharatdata import db
from bharatdata.ingest import runlog
from bharatdata.ingest.fetcher import FetchError, fetch_dataset
from bharatdata.ingest.schedule import FETCH_TASK_NAME, build_beat_schedule
from bharatdata.registry import DatasetEntry, load_registry
from bharatdata.sync_registry import upsert_entry

celery_app = Celery(
    "bharatdata",
    broker=os.environ.get("REDIS_URL", "redis://localhost:6379/0"),
)
celery_app.conf.task_ignore_result = True  # outcomes live in Postgres fetch_runs, not the result backend
# Registry crons are UTC.
celery_app.conf.timezone = "UTC"
celery_app.conf.enable_utc = True


def install_beat_schedule(app: Celery, registry: dict[str, DatasetEntry] | None = None) -> dict[str, Any]:
    """Generate the schedule from the registry and set it on ``app``.

    Loads (and fully validates) the registry itself unless one is passed, so a
    broken YAML stops the worker/beat at start-up instead of silently dropping
    a dataset from the schedule.
    """
    schedule = build_beat_schedule(registry if registry is not None else load_registry())
    app.conf.beat_schedule = schedule
    return schedule


install_beat_schedule(celery_app)


def run_logged_fetch(conn, entry: DatasetEntry, **fetch_kwargs: Any) -> dict[str, Any]:
    """Fetch ``entry`` with a ``fetch_runs`` row around it; failures are recorded, then re-raised."""
    with conn.cursor() as cur:
        dataset_id, _ = upsert_entry(cur, entry)  # the YAML is the source of truth; keeps the FK row current
    conn.commit()
    run_id = runlog.start_run(conn, dataset_id)
    try:
        result = fetch_dataset(entry, **fetch_kwargs)
    except BaseException as exc:
        pages = exc.pages if isinstance(exc, FetchError) else ()
        message = f"{type(exc).__name__}: {exc}"
        try:
            runlog.finish_failed(
                conn, run_id, message,
                raw_paths=[p.raw_path for p in pages],
                records_fetched=sum(p.record_count for p in pages),
            )
        except Exception:  # never let a logging problem mask the real failure
            conn.rollback()
        raise
    runlog.finish_succeeded(
        conn, run_id, raw_paths=result.raw_paths, records_fetched=result.records_fetched
    )
    return {**result.to_dict(), "fetch_run_id": run_id}


@celery_app.task(name=FETCH_TASK_NAME, shared=False)  # shared=False: do not leak into other Celery apps
def fetch_dataset_task(slug: str) -> dict[str, Any]:
    """Fetch one registry dataset into the raw zone and log the run; see the module docstring.

    Loads the whole registry first, so a broken registry file fails the task
    loudly instead of fetching from a half-valid catalogue.
    """
    registry = load_registry()
    if slug not in registry:
        raise KeyError(f"unknown dataset slug {slug!r}; known: {sorted(registry)}")
    conn = db.connect()
    try:
        return run_logged_fetch(conn, registry[slug])
    finally:
        conn.close()
