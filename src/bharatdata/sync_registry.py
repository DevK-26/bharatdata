"""Sync ``registry/*.yaml`` into the ``datasets`` table.

The YAML files are the source of truth; the table is a queryable copy of them.
Upserting is idempotent: re-running changes nothing (not even ``updated_at``)
unless an entry actually changed.

    python -m bharatdata.sync_registry                 # uses $DATABASE_URL, ./registry
    python -m bharatdata.sync_registry --registry DIR --database-url URL

Datasets are never deleted here: removing a YAML file does not remove the
catalogue row (snapshots reference it). Archiving is ``is_active = false``.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass

from psycopg2.extras import Json

from bharatdata import db
from bharatdata.registry import DatasetEntry, RegistryError, load_registry

# (column, DatasetEntry attribute). Order matters only for the SQL below.
_COLUMNS = (
    ("title", "title"),
    ("description", "description"),
    ("sector", "sector"),
    ("ministry", "ministry"),
    ("source_type", "source_type"),
    ("resource_id", "resource_id"),
    ("resource_id_status", "resource_id_status"),
    ("source_url", "source_url"),
    ("source_format", "format"),
    ("expected_schema", "expected_schema"),
    ("refresh_cron", "refresh_cron"),
    ("licence", "licence"),
    ("attribution", "attribution"),
)

_INSERT_COLUMNS = ", ".join(["slug"] + [c for c, _ in _COLUMNS])
_PLACEHOLDERS = ", ".join(["%s"] * (1 + len(_COLUMNS)))
_CHANGED_COLUMNS = ", ".join(f"datasets.{c}" for c, _ in _COLUMNS)
_EXCLUDED_COLUMNS = ", ".join(f"EXCLUDED.{c}" for c, _ in _COLUMNS)
_SET_CLAUSE = ", ".join(f"{c} = EXCLUDED.{c}" for c, _ in _COLUMNS)

# xmax = 0 on the returned row means it was inserted, not updated.
_UPSERT_SQL = f"""
INSERT INTO datasets ({_INSERT_COLUMNS}) VALUES ({_PLACEHOLDERS})
ON CONFLICT (slug) DO UPDATE SET {_SET_CLAUSE}
WHERE ({_CHANGED_COLUMNS}) IS DISTINCT FROM ({_EXCLUDED_COLUMNS})
RETURNING id, (xmax = 0) AS inserted
"""


@dataclass(frozen=True)
class SyncResult:
    inserted: tuple[str, ...]
    updated: tuple[str, ...]
    unchanged: tuple[str, ...]

    def __str__(self) -> str:
        return (
            f"registry sync: {len(self.inserted)} inserted, "
            f"{len(self.updated)} updated, {len(self.unchanged)} unchanged"
        )


def _values(entry: DatasetEntry) -> list[object]:
    values: list[object] = [entry.slug]
    for _, attribute in _COLUMNS:
        value = getattr(entry, attribute)
        values.append(Json(value) if attribute == "expected_schema" else value)
    return values


def upsert_entry(cur, entry: DatasetEntry) -> tuple[int, str]:
    """Upsert one entry on an open cursor; returns ``(dataset_id, outcome)``.

    ``outcome`` is ``inserted``, ``updated`` or ``unchanged``. An unchanged row
    is not written at all, so ``updated_at`` only moves when the YAML changed.
    The caller commits.
    """
    cur.execute(_UPSERT_SQL, _values(entry))
    row = cur.fetchone()
    if row is not None:
        return row[0], "inserted" if row[1] else "updated"
    cur.execute("SELECT id FROM datasets WHERE slug = %s", (entry.slug,))
    return cur.fetchone()[0], "unchanged"


def sync_registry(conn, registry: dict[str, DatasetEntry]) -> SyncResult:
    """Upsert every entry in one transaction (all or nothing)."""
    outcomes: dict[str, list[str]] = {"inserted": [], "updated": [], "unchanged": []}
    try:
        with conn.cursor() as cur:
            for slug, entry in sorted(registry.items()):
                _, outcome = upsert_entry(cur, entry)
                outcomes[outcome].append(slug)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return SyncResult(*(tuple(outcomes[k]) for k in ("inserted", "updated", "unchanged")))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Upsert registry/*.yaml into the datasets table.")
    parser.add_argument("--registry", help="registry directory (default: $BHARATDATA_REGISTRY_DIR or ./registry)")
    parser.add_argument("--database-url", help="Postgres URL (default: $DATABASE_URL)")
    args = parser.parse_args(argv)
    try:
        registry = load_registry(args.registry)
        conn = db.connect(args.database_url)
    except RegistryError as exc:
        print(f"registry is invalid, nothing synced:\n{exc}", file=sys.stderr)
        return 2
    except db.DatabaseNotConfigured as exc:
        print(str(exc), file=sys.stderr)
        return 2
    try:
        result = sync_registry(conn, registry)
    finally:
        conn.close()
    print(result)
    return 0


if __name__ == "__main__":
    sys.exit(main())
