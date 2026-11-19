"""Fetch a dataset from the data.gov.in resource API into the raw zone.

Design rules (BLUEPRINT §5, CLAUDE.md):

- **The raw zone is immutable.** Every HTTP response body is written
  byte-for-byte to ``<raw_dir>/<slug>/<UTC timestamp>.json`` *before* anything
  parses it for meaning, and is never rewritten. A ``.meta.json`` sidecar
  beside it records where it came from and its SHA-256. Files are created
  exclusively (``open(..., "xb")``): an existing file is never overwritten.
- **One file per page.** data.gov.in is paginated (``limit``/``offset``), and a
  response body cannot be merged with others without altering bytes, so each
  page is its own raw file. All pages of one run share a ``run_id`` in their
  sidecars, and :class:`FetchResult` lists them in order.
- **The API key is never stored.** It is read from ``DATA_GOV_IN_API_KEY``
  (or passed in by the caller), sent as a query parameter, and redacted from
  sidecars and error messages.
- **Failures are loud.** 429/5xx/network errors are retried with exponential
  backoff; when retries are exhausted, or the response is not what the API
  documents, a :class:`FetchError` says exactly what happened. Pages already
  stored stay on disk and are listed on ``FetchError.pages``.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

from bharatdata.registry import DatasetEntry

API_BASE_URL = "https://api.data.gov.in/resource"
API_KEY_ENV = "DATA_GOV_IN_API_KEY"
RAW_DIR_ENV = "BHARATDATA_RAW_DIR"
DEFAULT_RAW_DIR = Path("data/raw")

DEFAULT_PAGE_SIZE = 1000
DEFAULT_MAX_RETRIES = 5  # retries after the first attempt, so up to 6 attempts
DEFAULT_BACKOFF_BASE = 1.0  # seconds; waits are base * 2**retry_index
DEFAULT_BACKOFF_MAX = 60.0  # ceiling for any single wait, incl. Retry-After
DEFAULT_TIMEOUT = (10.0, 60.0)  # (connect, read) seconds
DEFAULT_MAX_PAGES = 10_000  # runaway-loop guard, not a data limit

_RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})
_TIMESTAMP_FORMAT = "%Y%m%dT%H%M%S.%fZ"


class FetchError(Exception):
    """A fetch could not be completed. ``pages`` holds raw pages stored before it failed."""

    def __init__(self, message: str, pages: tuple[RawPage, ...] = ()) -> None:
        super().__init__(message)
        self.pages = pages


class FetchRetriesExhausted(FetchError):
    """Every retry of one request failed with a retryable error (429, 5xx, network)."""


@dataclass(frozen=True)
class RawPage:
    """One raw-zone file: a single API response stored byte-for-byte."""

    raw_path: Path
    meta_path: Path
    sha256: str
    record_count: int
    offset: int
    limit: int
    fetched_at: str  # ISO 8601, UTC
    size_bytes: int


@dataclass(frozen=True)
class FetchResult:
    """Outcome of a completed fetch run (errors raise :class:`FetchError` instead)."""

    slug: str
    resource_id: str
    run_id: str
    started_at: str
    finished_at: str
    pages: tuple[RawPage, ...]
    records_fetched: int
    total_reported: int | None  # the API's own "total", if it sent one
    complete: bool  # False only if max_pages / max_records stopped the run early
    stop_reason: str

    @property
    def raw_paths(self) -> tuple[Path, ...]:
        return tuple(page.raw_path for page in self.pages)

    def to_dict(self) -> dict[str, Any]:
        """JSON-safe form (paths as strings), e.g. for a Celery return value."""
        data = asdict(self)
        for page in data["pages"]:
            page["raw_path"] = str(page["raw_path"])
            page["meta_path"] = str(page["meta_path"])
        return data


@dataclass
class _RunState:
    pages: list[RawPage] = field(default_factory=list)
    records: int = 0


def get_api_key(environ: dict[str, str] | None = None) -> str:
    """The data.gov.in API key from ``DATA_GOV_IN_API_KEY``; FetchError if unset/blank."""
    key = (environ if environ is not None else os.environ).get(API_KEY_ENV, "").strip()
    if not key:
        raise FetchError(
            f"{API_KEY_ENV} is not set. Get a free personal key on data.gov.in "
            "(log in > My Account) and put it in .env — never in code."
        )
    return key


def raw_zone_dir(raw_dir: str | Path | None = None) -> Path:
    """The raw-zone root: the argument, else $BHARATDATA_RAW_DIR, else ./data/raw."""
    if raw_dir is not None:
        return Path(raw_dir)
    return Path(os.environ.get(RAW_DIR_ENV) or DEFAULT_RAW_DIR)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _redact(text: str, api_key: str) -> str:
    return text.replace(api_key, "***")


def _public_url(url: str, params: dict[str, Any]) -> str:
    """The request URL with the api-key parameter left out (safe to persist)."""
    safe = {k: v for k, v in params.items() if k != "api-key"}
    return requests.Request("GET", url, params=safe).prepare().url or url


def _retry_wait(retry_index: int, response: requests.Response | None, base: float, cap: float) -> float:
    """Seconds to wait before retry ``retry_index`` (0-based); honours Retry-After on 429."""
    wait = base * (2**retry_index)
    if response is not None:
        header = response.headers.get("Retry-After", "")
        if header.strip().isdigit():
            wait = max(wait, float(header))
    return min(wait, cap)


def _get_with_retries(
    session: requests.Session,
    url: str,
    params: dict[str, Any],
    api_key: str,
    *,
    max_retries: int,
    backoff_base: float,
    backoff_max: float,
    timeout: tuple[float, float],
    sleep: Callable[[float], None],
) -> requests.Response:
    """GET with exponential backoff on 429/5xx/network errors; other failures raise at once."""
    shown = _public_url(url, params)
    attempts = max_retries + 1
    last_problem = "no attempt made"
    for attempt in range(attempts):
        response: requests.Response | None = None
        try:
            response = session.get(url, params=params, timeout=timeout)
        except (requests.ConnectionError, requests.Timeout) as exc:
            last_problem = _redact(f"{type(exc).__name__}: {exc}", api_key)
        else:
            if response.status_code == 200:
                return response
            if response.status_code not in _RETRY_STATUSES:
                raise FetchError(
                    f"{shown} returned HTTP {response.status_code}; not retryable "
                    "(401/403 usually means a bad or missing API key, 404 a wrong resource id)"
                )
            last_problem = f"HTTP {response.status_code}"
        if attempt < attempts - 1:
            sleep(_retry_wait(attempt, response, backoff_base, backoff_max))
    raise FetchRetriesExhausted(
        f"giving up on {shown} after {attempts} attempts; last error: {last_problem}"
    )


def _parse_page(body: bytes, shown_url: str) -> tuple[list[Any], int | None]:
    """(records, reported total) from a response body; FetchError if it is not API-shaped."""
    try:
        payload = json.loads(body)
    except ValueError as exc:
        raise FetchError(f"{shown_url} returned HTTP 200 but the body is not JSON ({exc})") from exc
    records = payload.get("records") if isinstance(payload, dict) else None
    if not isinstance(records, list):
        keys = sorted(payload) if isinstance(payload, dict) else type(payload).__name__
        raise FetchError(
            f"{shown_url} returned JSON without a 'records' list (found: {keys}); "
            "the API response shape is not what the fetcher expects"
        )
    total = payload.get("total")
    try:
        total_int = int(total) if total is not None else None
    except (TypeError, ValueError):
        total_int = None
    return records, total_int


def _store_page(
    directory: Path,
    body: bytes,
    *,
    fetched: datetime,
    source_url: str,
    run_id: str,
    slug: str,
    resource_id: str,
    offset: int,
    limit: int,
    record_count: int,
    http_status: int,
) -> RawPage:
    """Write ``body`` untouched plus its sidecar; never overwrite an existing file."""
    directory.mkdir(parents=True, exist_ok=True)
    while True:  # a name clash (same microsecond) moves to the next microsecond
        stem = fetched.strftime(_TIMESTAMP_FORMAT)
        raw_path = directory / f"{stem}.json"
        try:
            with raw_path.open("xb") as handle:
                handle.write(body)
                handle.flush()
                os.fsync(handle.fileno())
        except FileExistsError:
            fetched += timedelta(microseconds=1)
            continue
        break

    sha256 = hashlib.sha256(body).hexdigest()
    fetched_at = fetched.isoformat()
    meta = {
        "slug": slug,
        "resource_id": resource_id,
        "run_id": run_id,
        "source_url": source_url,
        "fetched_at": fetched_at,
        "sha256": sha256,
        "size_bytes": len(body),
        "record_count": record_count,
        "offset": offset,
        "limit": limit,
        "http_status": http_status,
        "payload_file": raw_path.name,
    }
    meta_path = directory / f"{stem}.meta.json"
    with meta_path.open("x", encoding="utf-8") as handle:
        json.dump(meta, handle, indent=2, sort_keys=True)
        handle.write("\n")
    return RawPage(
        raw_path=raw_path,
        meta_path=meta_path,
        sha256=sha256,
        record_count=record_count,
        offset=offset,
        limit=limit,
        fetched_at=fetched_at,
        size_bytes=len(body),
    )


def fetch_dataset(
    entry: DatasetEntry,
    api_key: str | None = None,
    *,
    raw_dir: str | Path | None = None,
    session: requests.Session | None = None,
    page_size: int = DEFAULT_PAGE_SIZE,
    max_records: int | None = None,
    max_pages: int = DEFAULT_MAX_PAGES,
    max_retries: int = DEFAULT_MAX_RETRIES,
    backoff_base: float = DEFAULT_BACKOFF_BASE,
    backoff_max: float = DEFAULT_BACKOFF_MAX,
    timeout: tuple[float, float] = DEFAULT_TIMEOUT,
    sleep: Callable[[float], None] = time.sleep,
    now: Callable[[], datetime] = _utcnow,
    base_url: str = API_BASE_URL,
) -> FetchResult:
    """Fetch every page of ``entry``'s resource into the raw zone.

    ``api_key`` defaults to ``$DATA_GOV_IN_API_KEY``. Pagination follows the
    records actually received (``offset += len(records)``), so a server that
    caps ``limit`` below ``page_size`` (the public sample key returns 10) is
    still read to the end. Raises :class:`FetchError` for an unfetchable entry,
    a missing key, a non-retryable HTTP status, a malformed response, or
    :class:`FetchRetriesExhausted` when retries run out.
    """
    if not entry.is_fetchable:
        raise FetchError(
            f"{entry.slug}: not fetchable — needs source_type 'api' and a verified "
            f"resource_id (status: {entry.resource_id_status}). Resource ids are never guessed; "
            "confirm it on data.gov.in first (see the TODO_VERIFY note in the registry file)."
        )
    if page_size < 1:
        raise ValueError("page_size must be at least 1")
    if max_records is not None and max_records < 1:
        raise ValueError("max_records must be at least 1")
    key = api_key if api_key is not None else get_api_key()
    if not key.strip():
        raise FetchError(f"{API_KEY_ENV} is blank; refusing to call the API without a key")

    resource_id = entry.resource_id or ""
    url = f"{base_url.rstrip('/')}/{resource_id}"
    directory = raw_zone_dir(raw_dir) / entry.slug
    http = session or requests.Session()
    started = now()
    run_id = started.strftime(_TIMESTAMP_FORMAT)
    state = _RunState()
    offset = 0
    total: int | None = None
    complete, stop_reason = True, "all records fetched"

    def fail(message: str, cause: FetchError) -> FetchError:
        error = type(cause)(f"{entry.slug}: {message}", tuple(state.pages))
        return error

    while True:
        if len(state.pages) >= max_pages:
            complete, stop_reason = False, f"stopped at max_pages={max_pages}"
            break
        limit = page_size if max_records is None else min(page_size, max_records - state.records)
        params = {"api-key": key, "format": "json", "limit": limit, "offset": offset}
        shown = _public_url(url, params)
        try:
            response = _get_with_retries(
                http, url, params, key,
                max_retries=max_retries, backoff_base=backoff_base,
                backoff_max=backoff_max, timeout=timeout, sleep=sleep,
            )
            body = response.content  # exact bytes as received (after transport decoding)
            records, reported = _parse_page(body, shown)
        except FetchError as exc:
            raise fail(str(exc), exc) from exc

        if reported is not None:
            total = reported
        if not records:
            stop_reason = "server returned an empty page"
            break

        state.pages.append(
            _store_page(
                directory, body,
                fetched=now(), source_url=shown, run_id=run_id, slug=entry.slug,
                resource_id=resource_id, offset=offset, limit=limit,
                record_count=len(records), http_status=response.status_code,
            )
        )
        state.records += len(records)
        offset += len(records)

        if max_records is not None and state.records >= max_records:
            if total is None or offset < total:
                complete, stop_reason = False, f"stopped at max_records={max_records}"
            break
        if total is not None and offset >= total:
            break
        if total is None and len(records) < limit:
            break

    return FetchResult(
        slug=entry.slug,
        resource_id=resource_id,
        run_id=run_id,
        started_at=started.isoformat(),
        finished_at=now().isoformat(),
        pages=tuple(state.pages),
        records_fetched=state.records,
        total_reported=total,
        complete=complete,
        stop_reason=stop_reason,
    )
