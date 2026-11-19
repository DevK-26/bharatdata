# BharatData — Build Sprint

Goal: turn the foundations into a public data platform — ingest, clean,
version and serve Government of India open data. Each phase ends with
running tests plus a measured artifact, and is reviewed by Khushi before
the next phase starts. Spec: `BLUEPRINT.md` (§4 datasets, §5 architecture,
§9 build plan). Rules: `CLAUDE.md`.

Phases map to blueprint §9 days: A = Days 1–2, B = Days 3–8, C = Days 9–15,
D = Days 16–21, E = Days 22–24.

## Needs Khushi herself (Claude cannot do these)

1. **Free data.gov.in personal API key.** Log in at data.gov.in > My Account
   and copy the key into `.env` as `DATA_GOV_IN_API_KEY`. Needed at the start
   of Phase C (resource-id verification, first real fetch). The public sample
   key returns at most 10 records and is not enough.
2. **Create the GitHub repo under `DevK-26`** when ready to push. Nothing has
   been pushed; this folder is not even a git repository yet.

(Later, Phase E: the domain name and the Oracle VM — same ones CSV Doctor uses.)

## Phase A — Foundations (Days 1–2) — DONE, 2026-11-16

What exists and how it was verified:

- Package layout, `pyproject.toml` / `requirements.txt` (a test keeps them in
  sync), `.env.example`, `.gitignore`. `pip install -e .` works in the `.venv`.
- **Registry** (`registry/*.yaml` + `src/bharatdata/registry.py`): five entries
  — daily mandi prices, real-time AQI, petrol/diesel by city, IMD rainfall,
  pincode directory. The loader rejects unknown keys, duplicate keys, bad
  cron, bad types, slug/file-name mismatch, unknown licences, and any guessed
  resource id. All five are `resource_id: null` / `TODO_VERIFY`.
- **Fetcher** (`src/bharatdata/ingest/fetcher.py`): limit/offset pagination,
  exponential backoff on 429/5xx/network errors (honours `Retry-After`),
  byte-for-byte raw zone with `.meta.json` sidecar (SHA-256, record count,
  source URL without the key), clear errors on exhausted retries.
- **Postgres schema** (`db/schema.sql`): datasets, fetch_runs, snapshots, users,
  api_keys. Executed against a real throwaway Postgres and probed with 25
  accept/reject cases (`scripts/verify_schema_pg.py`).
- Minimal FastAPI app (`/health`), Celery worker with one fetch task,
  `docker-compose.yml` (caddy, api, ingest, postgres, redis), `Dockerfile`, Caddyfile.
- Tests: all HTTP mocked; full suite green. Demo run against a local mock
  wrote an artifact under `data/raw/demo-fixture/`.

Not verified in Phase A (be honest about it): `docker compose up` / image
builds (no Docker in the sandbox), Caddyfile syntax (no Caddy binary), any
live data.gov.in behaviour.

## Phase B — Ingestion core + cleaning framework (Days 3–8)

**B1. Scheduler and run log** (BLUEPRINT Days 3–5)
- Celery Beat schedule generated from each entry's `refresh_cron`; add the
  `beat` service to compose.
- Write a `fetch_runs` row for every fetch (running → succeeded/failed, with
  `raw_paths`, record counts and the error text). A failed fetch must be
  visible, not silent. Small admin page (or JSON endpoint) listing recent
  runs and failures.
- Sync `registry/*.yaml` into the `datasets` table.
- Tests: scheduled task fires at the cron time (frozen clock); a failing
  fetch leaves a `failed` row; Postgres tests against a throwaway server.

**B2. Cleaning framework** (BLUEPRINT Days 6–8)
- Wire CSV Doctor's parse/profile/score as a library (read
  `~/workspace/csv-doctor` first; decide copy vs. shared package).
- Canonical state/district lists; date and number coercers.
- Per-dataset `clean()` contract: raw JSON in → typed dataframe + "what we
  fixed" list out. **Golden test per connector** (input raw → expected clean);
  no connector ships without one.

**B3. Snapshot + diff engine**
- Content-hash the cleaned data; create a `snapshots` row and a Parquet file
  only when the hash differs from the dataset's latest snapshot.
- Tests: same data twice → one snapshot; A → B → A → three snapshots.

## Phase C — First 5 datasets end-to-end + public API (Days 9–15)

**C1. Verify the five sources** (needs Khushi's API key)
- For each registry entry: find the real resource on data.gov.in, record the
  resource id, fetch ONE live sample page, confirm `expected_schema` against
  it, then flip `resource_id_status` to `VERIFIED`. Fix the schemas that were
  best-effort guesses.
- Open question: is petrol/diesel by city actually on data.gov.in with an
  API, or only on PPAC? If PPAC-only, add a file-based fetcher.
- Open question: IMD rainfall — district-wise daily vs subdivision-wise
  monthly; pick the MVP one.
- Acceptance: every dataset has a real raw-zone artifact from a real fetch and
  the registry says VERIFIED for exactly the ones confirmed.

**C2. Connectors**
- One `clean()` + golden test per dataset (mandi, AQI, fuel, rainfall, pincode).
  Pincode stays a string (leading zeros, identifiers are not numbers).
- End-to-end: scheduled fetch → raw → clean → snapshot, visible in `fetch_runs`.

**C3. Public API** (BLUEPRINT Days 13–15)
- `GET /v1/datasets`, `/datasets/{slug}`, `/datasets/{slug}/rows` (cursor
  pagination), `.csv` / `.parquet` downloads, `/snapshots`. DuckDB over Parquet.
- Every response carries `source`, `licence`, `attribution`.
- API keys by email signup (hash stored, never the key), 1,000/day per key,
  Redis counters, Redis cache keyed by (dataset, snapshot, query), revocation.
- OpenAPI polish and Python usage examples.
- Measured artifact: cache hit rate and p50/p95 latency from a local load
  test, written to `benchmarks/results/` by a script. Only measured numbers go
  in the README.

## Phase D — Website + datasets 6–20 (Days 16–21)

**D1. Next.js site** (same pattern as CSV Doctor's `apps/web`)
- Dataset page template: plain-language blurb, 100-row preview, charts
  (auto time-series + bar only), downloads, quality score + "what we fixed",
  freshness badge, snapshot history, citation box, API snippet.
- Search (Postgres FTS), home + sector pages, sitemap, structured metadata.
- Footer/page copy states "Not a Government of India product".
- Add `web` to compose and route it in the Caddyfile.

**D2. Datasets 6–20** in batches (BLUEPRINT §4), each with connector + clean() +
golden test + quality report before it ships. File-based sources first.

## Phase E — Deploy, launch, fix (Days 22–24)

**E1. Deploy to the VM** (needs Khushi: domain, VM, GitHub repo + secrets)
- GitHub Actions: tests, then deploy (copy CSV Doctor's workflow).
- Domain + HTTPS via Caddy; uptime and fetch-failure alerts to email;
  nightly Postgres + Parquet backup to a second bucket.
- First real `docker compose up` happens here — expect to fix things.

**E2. Launch content** — dev.to + Reddit posts, first LinkedIn "Dataset of
the week", Kaggle dataset #1.

**E3. Buffer** — fixes from the first real users; track the §10 metrics
(API keys, 7-day repeat callers, freshness %, inbound dataset requests).

## Resume bullets this sprint can produce (only once measured)

Nothing is claimable yet beyond "designed and tested the ingest foundations".
Fill these in only with real numbers from Phases B–E:

- Built an ingestion pipeline that fetches __ Government of India datasets on
  cron schedules into an immutable, hash-verified raw zone, with retry/backoff
  and a visible run log.
- Versioned cleaned data as content-hashed Parquet snapshots; queried with
  DuckDB behind a Redis-cached, rate-limited public API (cache hit rate __%,
  p95 __ ms).
- Golden-tested connectors for __ datasets; free API with __ keys issued and
  __ repeat callers after __ days.
