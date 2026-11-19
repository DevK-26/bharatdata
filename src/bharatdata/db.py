"""Postgres connection helper (psycopg2).

The connection string comes from ``DATABASE_URL`` — never from code. Callers
own the transaction: every writer in this package commits explicitly, so a
run-log row is durable the moment it is written.
"""

from __future__ import annotations

import os

import psycopg2

DATABASE_URL_ENV = "DATABASE_URL"


class DatabaseNotConfigured(RuntimeError):
    """``DATABASE_URL`` is not set, so nothing can be logged or synced."""


def connect(dsn: str | None = None):
    """Open a new connection to ``dsn`` or ``$DATABASE_URL``."""
    url = dsn or os.environ.get(DATABASE_URL_ENV, "").strip()
    if not url:
        raise DatabaseNotConfigured(
            f"{DATABASE_URL_ENV} is not set; the run log and the catalogue live in Postgres"
        )
    return psycopg2.connect(url)
