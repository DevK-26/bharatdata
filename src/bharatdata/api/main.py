"""BharatData public API.

Today: ``/health`` and the admin fetch-run log. Dataset and row endpoints
arrive in Phase C of BUILD-SPRINT.md. Do not add placeholder routes.
"""

from __future__ import annotations

import hmac
import os
from collections.abc import Iterator
from datetime import datetime
from typing import Annotated, Literal

from fastapi import Depends, FastAPI, Header, HTTPException, Query
from pydantic import BaseModel

from bharatdata import __version__, db
from bharatdata.ingest import runlog

ADMIN_TOKEN_ENV = "BHARATDATA_ADMIN_TOKEN"

app = FastAPI(
    title="BharatData API",
    version=__version__,
    description=(
        "Cleaned Government of India open data. Not a Government of India "
        "product; data sourced from the OGD Platform India under GODL."
    ),
)


@app.get("/health", tags=["meta"])
def health() -> dict[str, str]:
    """Liveness probe used by Docker and by uptime checks."""
    return {"status": "ok", "service": "bharatdata-api", "version": __version__}


def get_db() -> Iterator:
    """A request-scoped Postgres connection (override in tests via ``app.dependency_overrides``)."""
    try:
        conn = db.connect()
    except db.DatabaseNotConfigured as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    try:
        yield conn
    finally:
        conn.close()


def require_admin(x_admin_token: Annotated[str | None, Header()] = None) -> None:
    """Fail closed: with no ``BHARATDATA_ADMIN_TOKEN`` configured the admin API is off."""
    expected = os.environ.get(ADMIN_TOKEN_ENV, "")
    if not expected:
        raise HTTPException(status_code=503, detail=f"admin API disabled: {ADMIN_TOKEN_ENV} is not set")
    if x_admin_token is None or not hmac.compare_digest(x_admin_token.encode(), expected.encode()):
        raise HTTPException(status_code=401, detail="missing or wrong X-Admin-Token")


class FetchRun(BaseModel):
    id: int
    dataset_slug: str
    status: Literal["running", "succeeded", "failed"]
    started_at: datetime
    finished_at: datetime | None
    records_fetched: int
    pages_fetched: int
    raw_paths: list[str]
    error: str | None


class FetchRunList(BaseModel):
    count: int
    runs: list[FetchRun]


@app.get("/admin/fetch-runs", response_model=FetchRunList, tags=["admin"])
def list_fetch_runs(
    _: Annotated[None, Depends(require_admin)],
    conn=Depends(get_db),
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
    status: Literal["running", "succeeded", "failed"] | None = None,
    dataset: Annotated[str | None, Query(description="filter by dataset slug")] = None,
) -> FetchRunList:
    """Recent fetch runs, newest first — failures included, with their error text.

    Send the admin token in the ``X-Admin-Token`` header.
    """
    runs = runlog.list_runs(conn, limit=limit, status=status, slug=dataset)
    return FetchRunList(count=len(runs), runs=[FetchRun(**run) for run in runs])
