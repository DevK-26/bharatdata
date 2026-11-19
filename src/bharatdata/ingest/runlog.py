"""The fetch run log: one ``fetch_runs`` row per fetch attempt.

Lifecycle: :func:`start_run` inserts a ``running`` row and commits at once (so
a crash mid-fetch still leaves a visible ``running`` row), then exactly one of
:func:`finish_succeeded` / :func:`finish_failed` moves it to its final state.
The schema's CHECK constraints make a ``failed`` row without error text, or a
finished row without ``finished_at``, impossible to store.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

MAX_ERROR_CHARS = 4000  # keeps a pathological exception message from bloating the table


def start_run(conn, dataset_id: int) -> int:
    """Insert a ``running`` row and commit; returns the run id."""
    with conn.cursor() as cur:
        cur.execute("INSERT INTO fetch_runs (dataset_id) VALUES (%s) RETURNING id", (dataset_id,))
        run_id = cur.fetchone()[0]
    conn.commit()
    return run_id


def _finish(conn, run_id: int, *, status: str, records: int, paths: list[str], error: str | None) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE fetch_runs
               SET status = %s, finished_at = now(), records_fetched = %s,
                   pages_fetched = %s, raw_paths = %s, error = %s
             WHERE id = %s AND status = 'running'
            """,
            (status, records, len(paths), paths, error, run_id),
        )
        if cur.rowcount != 1:
            conn.rollback()
            raise RuntimeError(f"fetch run {run_id} is not in the 'running' state; refusing to overwrite it")
    conn.commit()


def finish_succeeded(conn, run_id: int, *, raw_paths: Iterable[Any], records_fetched: int) -> None:
    _finish(conn, run_id, status="succeeded", records=records_fetched,
            paths=[str(p) for p in raw_paths], error=None)


def finish_failed(
    conn, run_id: int, error: str, *, raw_paths: Iterable[Any] = (), records_fetched: int = 0
) -> None:
    """Mark the run failed. Pages stored before the failure stay listed (raw zone is immutable)."""
    text = error.strip() or "unknown error (empty message)"
    _finish(conn, run_id, status="failed", records=records_fetched,
            paths=[str(p) for p in raw_paths], error=text[:MAX_ERROR_CHARS])


def list_runs(conn, *, limit: int = 50, status: str | None = None, slug: str | None = None) -> list[dict[str, Any]]:
    """Most recent runs first, optionally filtered by status and/or dataset slug."""
    clauses, args = [], []
    if status is not None:
        clauses.append("r.status = %s")
        args.append(status)
    if slug is not None:
        clauses.append("d.slug = %s")
        args.append(slug)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT r.id, d.slug, r.status, r.started_at, r.finished_at,
                   r.records_fetched, r.pages_fetched, r.raw_paths, r.error
              FROM fetch_runs r JOIN datasets d ON d.id = r.dataset_id
              {where}
             ORDER BY r.started_at DESC, r.id DESC
             LIMIT %s
            """,
            (*args, limit),
        )
        rows = cur.fetchall()
    keys = ("id", "dataset_slug", "status", "started_at", "finished_at",
            "records_fetched", "pages_fetched", "raw_paths", "error")
    return [dict(zip(keys, row)) for row in rows]
