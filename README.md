# BharatData

India's government data exists but is hard to use. BharatData fetches it
from official sources (the OGD Platform India, data.gov.in), cleans it,
versions it, and republishes it as searchable dataset pages and a free
public API — under the Government Open Data License – India (GODL), with
attribution on every dataset.

Not a Government of India product. Full product spec: [BLUEPRINT.md](BLUEPRINT.md).
Build plan: [BUILD-SPRINT.md](BUILD-SPRINT.md).

## Status — read this first

**Foundations + scheduler and run log (Phases A and B1).** Honestly:

- **Not deployed.** The Docker Compose stack has been syntax-validated but never
  brought up (the new `beat` service included).
- **No live data.** No call to data.gov.in has ever been made. The fetcher is
  tested against mocked HTTP only.
- **All five registry entries are unverified.** Their data.gov.in resource ids are
  `null` / `TODO_VERIFY`, and their `expected_schema` is best-effort. The fetcher
  refuses to run a dataset until its id is verified — so today every scheduled run
  is logged as a `failed` run ("not fetchable"), visible in `/admin/fetch-runs`.
- **No cleaning, snapshots, public dataset endpoints or website yet** — only
  `/health` and the admin run log.

What exists and is tested: the dataset registry + validating loader, the
paginating/retrying raw-zone fetcher, the Postgres schema, a Celery Beat schedule
generated from each entry's `refresh_cron`, a `fetch_runs` run log (running →
succeeded/failed with the error text), registry → `datasets` sync, a FastAPI app
with `GET /admin/fetch-runs`, and the compose/Caddy stack definition.

## Architecture (target)

```
Internet -> Caddy (HTTPS) -> api   FastAPI: public API, keys, rate limits
                          -> web   Next.js dataset pages        (Phase D, not built)
            beat    Celery Beat: schedule generated from registry/*.yaml  (built, B1)
            ingest  Celery worker -> raw zone -> clean zone  (clean zone: Phase B2/B3)
            postgres  catalogue, users, API keys, snapshot + run metadata
            redis     cache, rate-limit counters, task queue
            Parquet files on disk, queried in place by DuckDB    (Phase B/C)
```

Design rules:

- **Raw zone is immutable.** Every fetch is stored byte-for-byte as
  `data/raw/<slug>/<UTC timestamp>.json` with a `.meta.json` sidecar (source URL,
  `fetched_at`, SHA-256, record count) before any cleaning.
- **Snapshots are content hashes.** A new snapshot exists only when the cleaned
  data actually changed.
- **Connectors are config + small code:** one `registry/<slug>.yaml` plus one
  `clean()` per dataset. No connector ships without a golden test.
- **Licence and attribution are schema fields** on every dataset, not a footer.

## Layout

| Path | What |
|---|---|
| `src/bharatdata/registry.py` | Loads + validates `registry/*.yaml`; fails loudly on any bad entry |
| `src/bharatdata/ingest/fetcher.py` | data.gov.in fetcher: pagination, retry/backoff, raw zone |
| `src/bharatdata/ingest/schedule.py` | Beat schedule generated from the registry's `refresh_cron` |
| `src/bharatdata/ingest/worker.py` | Celery app + the fetch task (wraps every fetch in a `fetch_runs` row) |
| `src/bharatdata/ingest/runlog.py` | `fetch_runs` writes + listing |
| `src/bharatdata/sync_registry.py` | `python -m bharatdata.sync_registry`: upsert registry YAML → `datasets` |
| `src/bharatdata/api/main.py` | FastAPI app: `/health`, `GET /admin/fetch-runs` (needs `X-Admin-Token`) |
| `registry/` | One YAML per dataset (first 5 of the blueprint) |
| `db/schema.sql` | Postgres schema |
| `data/raw/` | Raw-zone root (contents gitignored) |
| `docker-compose.yml`, `Dockerfile`, `deploy/caddy/` | caddy, api, ingest, beat, postgres, redis |
| `scripts/demo_fetch.py` | Fetcher end-to-end against a local mock (no network) |
| `tests/` | pytest; all HTTP mocked |

## Quickstart

Requires Python 3.12.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt   # runtime + test dependencies
pip install -e .                  # the bharatdata package

pytest                            # full suite, no network needed (see "Running the tests")

python scripts/demo_fetch.py      # writes a demo raw-zone artifact to data/raw/demo-fixture/
uvicorn bharatdata.api.main:app   # then GET http://127.0.0.1:8000/health
```

### Running the tests

HTTP is always mocked. The run-log, registry-sync and admin-endpoint tests run
against a **real throwaway Postgres** started by `pgserver` (a dev dependency; it
bundles the Postgres binaries and needs no system install). `pgserver` refuses to
run Postgres as root and drops to an unprivileged user, which cannot read a
root-only checkout; if you hit `error while loading shared libraries: libpq...`,
install it somewhere world-readable and put that first on the path:

```bash
pip install --target /tmp/bd-pgserver pgserver && chmod -R a+rX /tmp/bd-pgserver
PYTHONPATH=/tmp/bd-pgserver pytest
```

### Scheduler, run log, admin

```bash
python -m bharatdata.sync_registry     # upsert registry/*.yaml into the datasets table ($DATABASE_URL)
celery -A bharatdata.ingest.worker worker --loglevel=info   # the `ingest` service
celery -A bharatdata.ingest.worker beat   --loglevel=info   # the `beat` service (run exactly one)
curl -H "X-Admin-Token: $BHARATDATA_ADMIN_TOKEN" "http://127.0.0.1:8000/admin/fetch-runs?status=failed"
```

To fetch real data later you need a free personal data.gov.in API key (portal
login > My Account) in `DATA_GOV_IN_API_KEY` (copy `.env.example` to `.env`),
and a verified `resource_id` in the dataset's registry file.

## Licence and attribution

Datasets are republished under the Government Open Data License – India
(GODL), with the attribution recorded in each registry entry.
