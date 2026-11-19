"""Celery Beat schedule, generated from the registry.

There is no per-dataset schedule written by hand anywhere: every
``registry/<slug>.yaml`` entry yields exactly one beat entry whose cron is that
entry's ``refresh_cron``. Add a YAML file and it is scheduled; change its cron
and the schedule changes. Crons are evaluated in UTC (see ``worker.py``).
"""

from __future__ import annotations

from typing import Any

from celery.schedules import crontab

from bharatdata.registry import DatasetEntry

FETCH_TASK_NAME = "bharatdata.ingest.fetch_dataset"
ENTRY_PREFIX = "fetch-"


def entry_name(slug: str) -> str:
    return f"{ENTRY_PREFIX}{slug}"


def cron_to_schedule(expression: str) -> crontab:
    """A 5-field cron string -> Celery ``crontab`` (the registry loader already validated it)."""
    fields = expression.split()
    if len(fields) != 5:
        raise ValueError(f"refresh_cron {expression!r} must have 5 fields")
    minute, hour, day_of_month, month_of_year, day_of_week = fields
    return crontab(
        minute=minute,
        hour=hour,
        day_of_month=day_of_month,
        month_of_year=month_of_year,
        day_of_week=day_of_week,
    )


def build_beat_schedule(registry: dict[str, DatasetEntry]) -> dict[str, dict[str, Any]]:
    """One beat entry per registry dataset: ``{"fetch-<slug>": {task, schedule, args}}``."""
    return {
        entry_name(slug): {
            "task": FETCH_TASK_NAME,
            "schedule": cron_to_schedule(entry.refresh_cron),
            "args": (slug,),
        }
        for slug, entry in sorted(registry.items())
    }
