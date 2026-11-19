-- BharatData — Postgres schema (catalogue, run log, snapshots metadata, users, API keys).
--
-- Postgres holds what it is best at: the catalogue, users and keys, and the
-- *metadata* about snapshots. The data itself lives as Parquet files on disk
-- (queried in place by DuckDB) and the immutable raw zone under data/raw/.
--
-- Loaded once by the postgres container on first start
-- (docker-entrypoint-initdb.d). Plain CREATEs, no IF NOT EXISTS: a schema
-- change after first boot is a migration, not an edit-and-rerun of this file.
-- Timestamps are timestamptz and always UTC at the application edge.

BEGIN;

-- ---------------------------------------------------------------------------
-- Shared trigger: keep updated_at honest without trusting every writer.
-- ---------------------------------------------------------------------------
CREATE FUNCTION set_updated_at() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$;

-- ---------------------------------------------------------------------------
-- datasets — the catalogue. One row per registry/<slug>.yaml; the YAML files
-- are the source of truth and are synced into this table.
-- ---------------------------------------------------------------------------
CREATE TABLE datasets (
    id                  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    slug                text        NOT NULL UNIQUE
                        CHECK (slug ~ '^[a-z0-9]+(-[a-z0-9]+)*$'),
    title               text        NOT NULL CHECK (btrim(title) <> ''),
    description         text        NOT NULL CHECK (btrim(description) <> ''),
    sector              text        NOT NULL CHECK (sector ~ '^[a-z0-9]+(-[a-z0-9]+)*$'),
    ministry            text        NOT NULL CHECK (btrim(ministry) <> ''),

    -- Source info
    source_type         text        NOT NULL CHECK (source_type IN ('api', 'file')),
    resource_id         text        CHECK (resource_id ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'),
    resource_id_status  text        CHECK (resource_id_status IN ('VERIFIED', 'TODO_VERIFY')),
    source_url          text        NOT NULL CHECK (source_url ~ '^https?://'),
    source_format       text        NOT NULL CHECK (source_format IN ('json', 'csv', 'xml')),
    expected_schema     jsonb       NOT NULL CHECK (jsonb_typeof(expected_schema) = 'object'),

    -- Schedule: 5-field cron, evaluated in UTC (validated in full by the registry loader).
    refresh_cron        text        NOT NULL CHECK (array_length(regexp_split_to_array(btrim(refresh_cron), '\s+'), 1) = 5),

    -- Licence + attribution are part of the schema, not a footer (BLUEPRINT §7).
    licence             text        NOT NULL,
    attribution         text        NOT NULL CHECK (btrim(attribution) <> ''),

    -- Archive mode: a dead dataset stays published but is no longer fetched.
    is_active           boolean     NOT NULL DEFAULT true,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),

    -- Mirrors the registry rule: a resource id is never guessed.
    -- IS NOT DISTINCT FROM, not '=': a NULL status must make a branch false,
    -- because a CHECK that evaluates to NULL passes.
    CONSTRAINT datasets_resource_id_honest CHECK (
        (source_type = 'api'  AND resource_id IS NULL     AND resource_id_status IS NOT DISTINCT FROM 'TODO_VERIFY')
     OR (source_type = 'api'  AND resource_id IS NOT NULL AND resource_id_status IS NOT DISTINCT FROM 'VERIFIED')
     OR (source_type = 'file' AND resource_id IS NULL     AND resource_id_status IS NULL)
    ),

    -- Full-text search for the MVP (Postgres FTS; Meilisearch only if needed).
    search_tsv          tsvector GENERATED ALWAYS AS (
        to_tsvector('english', title || ' ' || description || ' ' || ministry)
    ) STORED
);

COMMENT ON TABLE  datasets IS 'Catalogue: one row per registry/<slug>.yaml dataset.';
COMMENT ON COLUMN datasets.slug IS 'URL-safe id; equals the registry file name and the /datasets/{slug} path.';
COMMENT ON COLUMN datasets.resource_id IS 'data.gov.in resource UUID. NULL until verified — never guessed.';
COMMENT ON COLUMN datasets.expected_schema IS 'Column name -> type, as declared in the registry; confirmed against the live resource before VERIFIED.';
COMMENT ON COLUMN datasets.refresh_cron IS '5-field cron, UTC; drives Celery Beat.';
COMMENT ON COLUMN datasets.is_active IS 'false = archive mode: still served, no longer fetched.';
COMMENT ON COLUMN datasets.search_tsv IS 'Generated tsvector over title, description and ministry for catalogue search.';

CREATE INDEX datasets_sector_idx   ON datasets (sector);
CREATE INDEX datasets_ministry_idx ON datasets (ministry);
CREATE INDEX datasets_search_idx   ON datasets USING gin (search_tsv);

CREATE TRIGGER datasets_set_updated_at
    BEFORE UPDATE ON datasets
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ---------------------------------------------------------------------------
-- fetch_runs — the run log. A failed fetch must be visible, not silent.
-- ---------------------------------------------------------------------------
CREATE TABLE fetch_runs (
    id                  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset_id          bigint      NOT NULL REFERENCES datasets (id) ON DELETE CASCADE,
    started_at          timestamptz NOT NULL DEFAULT now(),
    finished_at         timestamptz,
    status              text        NOT NULL DEFAULT 'running'
                        CHECK (status IN ('running', 'succeeded', 'failed')),
    records_fetched     bigint      NOT NULL DEFAULT 0 CHECK (records_fetched >= 0),
    pages_fetched       integer     NOT NULL DEFAULT 0 CHECK (pages_fetched >= 0),
    -- Raw-zone files this run wrote (data/raw/<slug>/<timestamp>.json), in page order.
    raw_paths           text[]      NOT NULL DEFAULT '{}',
    error               text,

    CONSTRAINT fetch_runs_finished_matches_status CHECK (
        (status = 'running' AND finished_at IS NULL) OR (status <> 'running' AND finished_at IS NOT NULL)
    ),
    CONSTRAINT fetch_runs_failed_has_error CHECK (
        (status = 'failed') = (error IS NOT NULL)
    ),
    CONSTRAINT fetch_runs_finished_after_started CHECK (finished_at IS NULL OR finished_at >= started_at)
);

COMMENT ON TABLE  fetch_runs IS 'One row per fetch attempt per dataset; the admin page and freshness badges read this.';
COMMENT ON COLUMN fetch_runs.error IS 'Human-readable failure reason; set exactly when status = failed.';
COMMENT ON COLUMN fetch_runs.raw_paths IS 'Raw-zone payload files written by this run, so any run can be re-cleaned from raw.';

-- "Latest run for dataset X" and the per-dataset history list.
CREATE INDEX fetch_runs_dataset_started_idx ON fetch_runs (dataset_id, started_at DESC);
-- The failures view stays small and fast however many runs succeed.
CREATE INDEX fetch_runs_failed_idx ON fetch_runs (started_at DESC) WHERE status = 'failed';
-- Spot runs that crashed without ever finishing.
CREATE INDEX fetch_runs_running_idx ON fetch_runs (started_at) WHERE status = 'running';

-- ---------------------------------------------------------------------------
-- snapshots — versions of the CLEANED data. A snapshot is a content hash:
-- one is created only when the cleaned data actually changed.
-- ---------------------------------------------------------------------------
CREATE TABLE snapshots (
    id                  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset_id          bigint      NOT NULL REFERENCES datasets (id) ON DELETE RESTRICT,
    content_hash        text        NOT NULL CHECK (content_hash ~ '^[0-9a-f]{64}$'),
    row_count           bigint      NOT NULL CHECK (row_count >= 0),
    parquet_path        text        NOT NULL CHECK (btrim(parquet_path) <> ''),
    -- The fetch that produced the raw input; NULL if that log row was later pruned.
    fetch_run_id        bigint      REFERENCES fetch_runs (id) ON DELETE SET NULL,
    created_at          timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE  snapshots IS 'Metadata for versions of cleaned data; the data itself is Parquet at parquet_path.';
COMMENT ON COLUMN snapshots.content_hash IS 'SHA-256 (hex) of the cleaned data. The application creates a snapshot only when this differs from the dataset''s latest one.';
COMMENT ON COLUMN snapshots.parquet_path IS 'Path of the Parquet file, relative to the clean-zone root.';

-- Deliberately NOT unique on (dataset_id, content_hash): data may legitimately
-- return to an earlier state (A -> B -> A). "Changed" means "differs from the
-- latest snapshot", which the ingest code checks inside a transaction.
CREATE INDEX snapshots_dataset_created_idx ON snapshots (dataset_id, created_at DESC);
CREATE INDEX snapshots_content_hash_idx    ON snapshots (content_hash);
CREATE INDEX snapshots_fetch_run_idx       ON snapshots (fetch_run_id);

-- ---------------------------------------------------------------------------
-- users — people who signed up. Signup is by email; keys work without a user
-- row being required (see api_keys.user_id).
-- ---------------------------------------------------------------------------
CREATE TABLE users (
    id                  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    email               text        NOT NULL CHECK (email ~ '^[^@\s]+@[^@\s]+$'),
    created_at          timestamptz NOT NULL DEFAULT now(),
    last_seen_at        timestamptz
);

COMMENT ON TABLE users IS 'Signed-up users (email only). No passwords stored; no personal data beyond the email.';

-- Case-insensitive uniqueness without needing the citext extension.
CREATE UNIQUE INDEX users_email_lower_key ON users (lower(email));

-- ---------------------------------------------------------------------------
-- api_keys — the free tier. Only a hash of the key is stored; the plaintext
-- is shown once at creation and cannot be recovered.
-- ---------------------------------------------------------------------------
CREATE TABLE api_keys (
    id                  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    -- SHA-256 hex of the key. Keys are long random strings, so a fast unsalted
    -- hash is appropriate here (unlike passwords) and keeps lookup one index probe.
    key_hash            text        NOT NULL UNIQUE CHECK (key_hash ~ '^[0-9a-f]{64}$'),
    -- First few characters of the key, so a user can tell their keys apart.
    key_prefix          text        NOT NULL CHECK (btrim(key_prefix) <> ''),
    -- Address the key was issued to. Kept on the key itself because signup is
    -- email-only; user_id links it to a users row when one exists.
    email               text        NOT NULL CHECK (email ~ '^[^@\s]+@[^@\s]+$'),
    user_id             bigint      REFERENCES users (id) ON DELETE SET NULL,
    daily_limit         integer     NOT NULL DEFAULT 1000 CHECK (daily_limit > 0),
    revoked             boolean     NOT NULL DEFAULT false,
    revoked_at          timestamptz,
    created_at          timestamptz NOT NULL DEFAULT now(),
    last_used_at        timestamptz,

    CONSTRAINT api_keys_revoked_has_timestamp CHECK (revoked = (revoked_at IS NOT NULL))
);

COMMENT ON TABLE  api_keys IS 'Free-tier API keys (BLUEPRINT §3b): 1,000 calls/day by default, revocable.';
COMMENT ON COLUMN api_keys.key_hash IS 'SHA-256 hex of the plaintext key; the plaintext is never stored.';
COMMENT ON COLUMN api_keys.daily_limit IS 'Calls per UTC day; enforced with Redis counters, not by this table.';
COMMENT ON COLUMN api_keys.revoked IS 'true once an admin (or abuse handling) has revoked the key; revoked_at records when.';

CREATE INDEX api_keys_email_idx   ON api_keys (lower(email));
CREATE INDEX api_keys_user_idx    ON api_keys (user_id) WHERE user_id IS NOT NULL;
CREATE INDEX api_keys_revoked_idx ON api_keys (revoked_at) WHERE revoked;

COMMIT;
