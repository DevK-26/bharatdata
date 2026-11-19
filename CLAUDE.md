# BharatData — build rules

BharatData is Khushi's second independent product (after CSV Doctor):
cleaned Government of India open data, republished as dataset pages + a
free public API. Full product spec: BLUEPRINT.md in this folder — read it
before building anything.

## Quality bar (non-negotiable)

- A phase is done ONLY when: the implementation is complete (no
  placeholders, no TODO stubs, no "left as an exercise"), the new
  behaviour has tests, the FULL test suite passes on your own re-run,
  and any measured artifact the phase promises exists and is real.
- If a check fails, fix it inside the same run and re-verify. Never
  report success over a failing check.
- Honesty over fake precision: never invent data.gov.in resource UUIDs,
  statistics, or benchmark numbers. Unknown values are marked
  TODO_VERIFY with a note on how they will be confirmed.
- No live network calls in tests — mock all HTTP. No API keys in code;
  keys come from environment variables only.

## Design rules (from the blueprint)

- Raw zone is immutable: every fetch is stored byte-for-byte with a
  timestamp + SHA-256 hash before any cleaning runs.
- Snapshots are content hashes: a new snapshot exists only when the
  cleaned data actually changed.
- Parquet + DuckDB for analytical queries; Postgres for catalogue,
  users, API keys. Redis for cache + rate limits + queues.
- Every dataset page/API response carries source, licence (GODL) and
  attribution fields — part of the schema, not a footer.
- Connectors are config + small code: one registry YAML + one clean()
  per dataset. No connector ships without a golden test.

## Stack

Python (FastAPI API, Celery ingest), Postgres, Redis, Caddy, Next.js
web — same shape as the CSV Doctor stack in ~/workspace/csv-doctor
(read its docker-compose.yml and deploy/ folder for the proven pattern).
