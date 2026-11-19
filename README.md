# BharatData

India's government data exists but is hard to use. BharatData fetches it
from official sources (the OGD Platform India, data.gov.in), cleans it,
versions it, and republishes it as searchable dataset pages and a free
public API — under the Government Open Data License – India (GODL), with
attribution on every dataset.

Not a Government of India product. Full product spec: [BLUEPRINT.md](BLUEPRINT.md).
Build plan: [BUILD-SPRINT.md](BUILD-SPRINT.md).

## Status — read this first

**Foundations only (Phase A).** Honestly:

- **Not deployed.** The Docker Compose stack has been syntax-validated but never
  brought up.
- **No live data.** No call to data.gov.in has ever been made. The fetcher is
  tested against mocked HTTP only.
- **All five registry entries are unverified.** Their data.gov.in resource ids are
  `null` / `TODO_VERIFY`, and their `expected_schema` is best-effort. The fetcher
  refuses to run a dataset until its id is verified.
- **No cleaning, snapshots, public API endpoints or website yet** — only
  `/health`.

What exists and is tested: the dataset registry + validating loader, the
paginating/retrying raw-zone fetcher, the Postgres schema, a minimal FastAPI app
and Celery worker, and the compose/Caddy stack definition.

## Architecture (target)

```
Internet -> Caddy (HTTPS) -> api   FastAPI: public API, keys, rate limits
                          -> web   Next.js dataset pages        (Phase D, not built)
            ingest  Celery worker (+ Beat, Phase B) -> raw zone -> clean zone
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
| `src/bharatdata/ingest/worker.py` | Celery app + one fetch task |
| `src/bharatdata/api/main.py` | FastAPI app (`/health` only) |
| `registry/` | One YAML per dataset (first 5 of the blueprint) |
| `db/schema.sql` | Postgres schema |
| `data/raw/` | Raw-zone root (contents gitignored) |
| `docker-compose.yml`, `Dockerfile`, `deploy/caddy/` | caddy, api, ingest, postgres, redis |
| `scripts/demo_fetch.py` | Fetcher end-to-end against a local mock (no network) |
| `tests/` | pytest; all HTTP mocked |

## Quickstart

Requires Python 3.12.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt   # runtime + test dependencies
pip install -e .                  # the bharatdata package

pytest                            # full suite, no network needed

python scripts/demo_fetch.py      # writes a demo raw-zone artifact to data/raw/demo-fixture/
uvicorn bharatdata.api.main:app   # then GET http://127.0.0.1:8000/health
```

To fetch real data later you need a free personal data.gov.in API key (portal
login > My Account) in `DATA_GOV_IN_API_KEY` (copy `.env.example` to `.env`),
and a verified `resource_id` in the dataset's registry file.

## Licence and attribution

Datasets are republished under the Government Open Data License – India
(GODL), with the attribution recorded in each registry entry.
