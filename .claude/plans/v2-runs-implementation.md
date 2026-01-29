# V2 Runs Implementation Plan

**Target Repo:** TRR-Backend

## Decisions Made

| Decision | Answer |
|----------|--------|
| TRR Integration | **HTTP read/write** (service-to-service) with TRR as run-state owner |
| Database Schema | **Need Supabase SQL migrations** in a dedicated `screenalytics` schema |
| PR Strategy | **Multiple PRs** (P0 → P1 → P2; do not combine) |

---

## Current State Summary

**SCREENALYTICS PRs merged to main:**
- `047c78d` - fix: make artifacts resolver robust
- `2003443` - chore: deprecate legacy refresh endpoints
- `bb0ac36` - fix(config): robust REPO_ROOT resolution

**V2 infrastructure is stubbed (SCREENALYTICS):**
- `RunsV2Service` - ~30% (no DB ops)
- `TRRIngestService` - ~5% (stubs only)
- Celery v2 tasks - 0%
- V2 router endpoints - all return 501

**TRR-Backend already has:**
- `pipeline` schema (`pipeline.runs`, `pipeline.run_stages`)
- Screenalytics support views: `core.v_episode_cast`, `core.v_season_cast`, `core.v_person_images`

**TRR-Backend does NOT have:**
- `screenalytics.*` schema + tables
- Screenalytics ingest endpoints in FastAPI

---

## PR P0: Supabase Migrations (screenalytics.* schema + tables)

**Files**
- `supabase/migrations/0093_create_screenalytics_v2_runs.sql` (next available number)

**Database changes (explicit + safe)**

```sql
-- supabase/migrations/0093_create_screenalytics_v2_runs.sql

BEGIN;

CREATE SCHEMA IF NOT EXISTS screenalytics;

-- A) screenalytics.video_assets (episode/season/show assets)
CREATE TABLE screenalytics.video_assets (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    episode_id uuid NULL REFERENCES core.episodes(id) ON DELETE CASCADE,
    season_id uuid NULL REFERENCES core.seasons(id) ON DELETE CASCADE,
    show_id uuid NULL REFERENCES core.shows(id) ON DELETE CASCADE,
    media_asset_id uuid NULL REFERENCES core.media_assets(id) ON DELETE SET NULL,
    source_url text NULL,
    duration_seconds numeric NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK (episode_id IS NOT NULL OR season_id IS NOT NULL OR show_id IS NOT NULL),
    CHECK (media_asset_id IS NOT NULL OR source_url IS NOT NULL)
);

CREATE INDEX screenalytics_video_assets_episode_idx ON screenalytics.video_assets (episode_id);
CREATE INDEX screenalytics_video_assets_season_idx ON screenalytics.video_assets (season_id);
CREATE INDEX screenalytics_video_assets_show_idx ON screenalytics.video_assets (show_id);

-- B) screenalytics.runs_v2
CREATE TABLE screenalytics.runs_v2 (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    video_asset_id uuid NOT NULL REFERENCES screenalytics.video_assets(id) ON DELETE RESTRICT,
    status text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending','running','success','failed','cancelled')),
    run_config_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    config_hash text NULL,
    candidate_cast_snapshot_json jsonb NOT NULL DEFAULT '[]'::jsonb,
    manifest_key text NULL,
    started_at timestamptz NULL,
    completed_at timestamptz NULL,
    error_message text NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX screenalytics_runs_v2_status_idx ON screenalytics.runs_v2 (status);
CREATE INDEX screenalytics_runs_v2_video_asset_idx ON screenalytics.runs_v2 (video_asset_id);

-- C) screenalytics.run_artifacts
CREATE TABLE screenalytics.run_artifacts (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id uuid NOT NULL REFERENCES screenalytics.runs_v2(id) ON DELETE CASCADE,
    artifact_key text NOT NULL,
    artifact_kind text NOT NULL,
    s3_key text NOT NULL,
    schema_version text NULL,
    content_type text NULL,
    checksum_sha256 text NULL,
    row_count bigint NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (run_id, artifact_key)
);

CREATE INDEX screenalytics_run_artifacts_run_idx ON screenalytics.run_artifacts (run_id);

-- D) screenalytics.run_person_metrics
CREATE TABLE screenalytics.run_person_metrics (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id uuid NOT NULL REFERENCES screenalytics.runs_v2(id) ON DELETE CASCADE,
    person_id uuid NOT NULL REFERENCES core.people(id) ON DELETE RESTRICT,
    screen_time_seconds numeric NOT NULL DEFAULT 0,
    frame_count integer NOT NULL DEFAULT 0,
    confidence_avg numeric NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (run_id, person_id)
);

CREATE INDEX screenalytics_run_person_metrics_run_idx ON screenalytics.run_person_metrics (run_id);
CREATE INDEX screenalytics_run_person_metrics_person_idx ON screenalytics.run_person_metrics (person_id);

-- E) screenalytics.unknown_clusters
CREATE TABLE screenalytics.unknown_clusters (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id uuid NOT NULL REFERENCES screenalytics.runs_v2(id) ON DELETE CASCADE,
    cluster_id text NOT NULL,
    track_count integer NOT NULL DEFAULT 0,
    preview_s3_key text NULL,
    assigned_person_id uuid NULL REFERENCES core.people(id) ON DELETE RESTRICT,
    assigned_by text NULL,
    assigned_at timestamptz NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (run_id, cluster_id)
);

CREATE INDEX screenalytics_unknown_clusters_run_idx ON screenalytics.unknown_clusters (run_id);
CREATE INDEX screenalytics_unknown_clusters_unassigned_idx
    ON screenalytics.unknown_clusters (run_id)
    WHERE assigned_person_id IS NULL;

-- updated_at triggers (use existing core.set_updated_at)
CREATE TRIGGER screenalytics_video_assets_set_updated_at
BEFORE UPDATE ON screenalytics.video_assets
FOR EACH ROW EXECUTE FUNCTION core.set_updated_at();

CREATE TRIGGER screenalytics_runs_v2_set_updated_at
BEFORE UPDATE ON screenalytics.runs_v2
FOR EACH ROW EXECUTE FUNCTION core.set_updated_at();

CREATE TRIGGER screenalytics_run_artifacts_set_updated_at
BEFORE UPDATE ON screenalytics.run_artifacts
FOR EACH ROW EXECUTE FUNCTION core.set_updated_at();

CREATE TRIGGER screenalytics_run_person_metrics_set_updated_at
BEFORE UPDATE ON screenalytics.run_person_metrics
FOR EACH ROW EXECUTE FUNCTION core.set_updated_at();

CREATE TRIGGER screenalytics_unknown_clusters_set_updated_at
BEFORE UPDATE ON screenalytics.unknown_clusters
FOR EACH ROW EXECUTE FUNCTION core.set_updated_at();

-- RLS + grants (service-to-service only)
GRANT USAGE ON SCHEMA screenalytics TO service_role;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA screenalytics TO service_role;

ALTER TABLE screenalytics.video_assets ENABLE ROW LEVEL SECURITY;
ALTER TABLE screenalytics.runs_v2 ENABLE ROW LEVEL SECURITY;
ALTER TABLE screenalytics.run_artifacts ENABLE ROW LEVEL SECURITY;
ALTER TABLE screenalytics.run_person_metrics ENABLE ROW LEVEL SECURITY;
ALTER TABLE screenalytics.unknown_clusters ENABLE ROW LEVEL SECURITY;

CREATE POLICY "service_role_all_video_assets"
ON screenalytics.video_assets FOR ALL TO service_role
USING (true) WITH CHECK (true);

CREATE POLICY "service_role_all_runs_v2"
ON screenalytics.runs_v2 FOR ALL TO service_role
USING (true) WITH CHECK (true);

CREATE POLICY "service_role_all_run_artifacts"
ON screenalytics.run_artifacts FOR ALL TO service_role
USING (true) WITH CHECK (true);

CREATE POLICY "service_role_all_run_person_metrics"
ON screenalytics.run_person_metrics FOR ALL TO service_role
USING (true) WITH CHECK (true);

CREATE POLICY "service_role_all_unknown_clusters"
ON screenalytics.unknown_clusters FOR ALL TO service_role
USING (true) WITH CHECK (true);

COMMIT;
```

**Verification commands**
```bash
supabase db reset --yes
psql -c "\dt screenalytics.*"
psql -c "SELECT column_name, is_nullable FROM information_schema.columns WHERE table_schema='screenalytics' AND table_name='runs_v2' ORDER BY ordinal_position;"
```

**Acceptance criteria (P0)**
- Migration applies cleanly on `supabase db reset`.
- Constraints exist:
  - `runs_v2.video_asset_id NOT NULL`
  - `video_assets CHECK (episode_id OR season_id OR show_id)`
  - `video_assets CHECK (media_asset_id OR source_url)`
  - `run_artifacts UNIQUE(run_id, artifact_key)`
  - `run_person_metrics UNIQUE(run_id, person_id)` + FK to `core.people`
  - `unknown_clusters UNIQUE(run_id, cluster_id)`
- RLS enabled + service_role policies present on all tables.

---

## PR P1: Service-token ingest endpoints (cast/photos)

**Files**
- `api/routers/screenalytics.py` (new)
- `api/deps.py` (add token dependency)
- `tests/api/test_screenalytics_ingest_endpoints.py`

**Auth dependency**
- Require header: `Authorization: Bearer <token>`
- Env var: `SCREENALYTICS_SERVICE_TOKEN`
- Missing token → 401
- Wrong token → 401
- Use constant-time compare (`hmac.compare_digest`)

**Endpoints (prefix `/api/v1/screenalytics`)**
- `GET /episodes/{episode_id}/cast`
  - Source: `core.v_episode_cast`
  - Query params: `credit_category` (optional), `limit`, `offset`
- `GET /seasons/{season_id}/cast`
  - Source: `core.v_season_cast`
- `GET /people/{person_id}/photos`
  - Source: `core.v_person_images`
  - Return: `served_url`, `hosted_key`, `is_primary`, `width`, `height`, `kind` (if present)

**Tests**
- 401 without token
- 401 with wrong token
- 200 with token
- Response JSON shape sanity (do not assume non-empty)

**Acceptance criteria (P1)**
- Endpoints return 200 locally with valid token and expected keys.
- Endpoints return 401 without valid token.
- Tests pass.

---

## PR P2: runs_v2 + artifacts + metrics + unknown clusters API (service-to-service)

**Files**
- `trr_backend/repositories/screenalytics_runs.py` (new; Supabase admin client wrapper)
- `api/routers/screenalytics_runs_v2.py` (new, or extend screenalytics.py)
- `tests/api/test_screenalytics_runs_v2.py`

**Auth**
- All P2 endpoints must use the same service-token dependency (401 on missing/wrong token).

**Endpoints (prefix `/api/v1/screenalytics/v2`)**

A) Video assets
- `POST /video-assets`
  - body: `episode_id?`, `season_id?`, `show_id?`, `media_asset_id?`, `source_url?`, `duration_seconds?`, `metadata`
  - enforce: `(episode_id OR season_id OR show_id)` AND `(media_asset_id OR source_url)`
- `GET /video-assets/{video_asset_id}`

B) Runs
- `POST /runs`
  - body: `video_asset_id`, `run_config_json`, `config_hash?`, `candidate_cast_snapshot_json?`
  - server MAY compute `config_hash` if omitted (safe)
- `GET /runs/{run_id}`
  - MUST include expanded video_asset fields to avoid a second call:
    - `episode_id`, `season_id`, `show_id`, `media_asset_id`, `source_url`, `duration_seconds`, `metadata`
  - plus run fields: `run_config_json`, `config_hash`, `candidate_cast_snapshot_json`, `status`, timestamps, `error_message`, `manifest_key`
- `PATCH /runs/{run_id}/status`
  - body: `status`, `error_message?`, `manifest_key?`
  - set `started_at` when first transitioning into `running`
  - set `completed_at` when transitioning into terminal (`success`/`failed`/`cancelled`)

C) Artifacts (idempotent upsert)
- `POST /runs/{run_id}/artifacts:upsert`
  - body: list of artifacts
  - upsert on `(run_id, artifact_key)`

D) Metrics (idempotent upsert)
- `POST /runs/{run_id}/person-metrics:upsert`
  - body: list of `{person_id, screen_time_seconds, frame_count, confidence_avg, metadata}`
  - upsert on `(run_id, person_id)`

E) Unknown clusters
- `GET /runs/{run_id}/unknown-clusters`
- `POST /runs/{run_id}/unknown-clusters:upsert`
  - body: list of `{cluster_id, track_count, preview_s3_key?, metadata?}`
  - upsert on `(run_id, cluster_id)`
- `POST /runs/{run_id}/unknown-clusters/{cluster_id}/assign`
  - body: `{person_id, assigned_by}`
  - updates `assigned_person_id`, `assigned_by`, `assigned_at`

F) Leaderboard
- `GET /runs/{run_id}/leaderboard`
  - ordered by `screen_time_seconds DESC`

**Tests (P2)**
- Auth enforced (401 without token)
- Create video asset → create run → update status → upsert artifacts → upsert metrics → leaderboard sorted
- Unknown clusters: upsert clusters → assign cluster → GET shows assignment fields set
- Idempotency: upsert twice does not create duplicates (artifacts/metrics/clusters)

**Acceptance criteria (P2)**
- Screenalytics can run end-to-end via HTTP without DB assumptions.
- `GET /runs/{run_id}` returns video URL info + episode/season/show linkage.
- Artifacts/metrics/clusters upserts are idempotent (no duplicates).
- All tests pass.
