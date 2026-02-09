# Screenalytics Data Layer Unification — Task 5 Plan

Repo: TRR-Backend
Last updated: February 9, 2026

## Goal

Add missing screenalytics tables to Supabase (migrations 0102-0105) and verify TRR-Backend integration files are compatible with screenalytics' switch to Supabase-only data access.

## Status Snapshot

Complete. Merged via PR #48; migrations 0102–0105 applied on staging; drift reconciled via 0115.

## Scope

### Phase 1: Migrate Missing Tables to Supabase

Add the tables that exist in screenalytics' local Docker Postgres but not yet in Supabase. TRR-Backend's highest existing migration is 0101.

#### Phase 1 Preflight: Detect Schema Drift

Because `screenalytics/tools/dev-up.sh` can apply local `db/migrations/*.sql` directly to `TRR_DB_URL` (when `SCREENALYTICS_APPLY_MIGRATIONS=1`), the Supabase DB may already contain some of these tables from ad-hoc runs.

Before adding migrations 0102-0105:
- Check whether each target table already exists in Supabase (`to_regclass(...)`).
- If it exists, compare columns/constraints/indexes to the expected schema and write an `ALTER TABLE` reconciliation migration.
- Do this on a staging Supabase instance first.

```sql
SELECT
  to_regclass('screenalytics.face_bank_images') AS face_bank_images,
  to_regclass('screenalytics.video_asset_cast_candidates') AS video_asset_cast_candidates,
  to_regclass('screenalytics.outbox_events') AS outbox_events,
  to_regclass('screenalytics.runs') AS runs_v1;
```

#### New Migrations

- **Migration 0102**: `screenalytics.face_bank_images` — person-linked face images for facebank. `person_id` references `core.people(id)` directly (no `trr_person_id` mapping).
- **Migration 0103**: `screenalytics.video_asset_cast_candidates` — expected cast per video asset. FK to `screenalytics.video_assets(id)` and `core.people(id)`.
- **Migration 0104**: `screenalytics.runs_v1` — 6 v1 operational tables (`runs`, `job_runs`, `identity_locks`, `suggestion_batches`, `suggestions`, `suggestion_applies`). Note: `ep_id` is a screenalytics-internal episode token, not `core.episodes.id`.
- **Migration 0105**: `screenalytics.outbox_events` — event delivery queue.

RLS for all new tables: service_role only (same pattern as existing screenalytics tables in migration 0093).

### Phase 3 (Partial): Verify TRR-Backend Integration Files

TRR-Backend has 6 files that face screenalytics. Verify they are compatible after screenalytics switches to Supabase-only access:

- `api/routers/screenalytics.py` — Ingest endpoints (cast, photos)
- `api/routers/screenalytics_runs_v2.py` — v2 run state management
- `trr_backend/repositories/screenalytics_runs.py` — Direct SQL to `screenalytics.*` tables; verify column names match resolved schema (Supabase migration 0093 uses `id` as PK, not `video_asset_id`/`run_id`)
- `trr_backend/clients/screenalytics.py` — HTTP client
- `trr_backend/pipeline/stages/sync_screenalytics.py` — Pipeline stage (currently stubbed)
- `api/screenalytics_auth.py` — Service token auth via `SCREENALYTICS_SERVICE_TOKEN`

## Out of Scope

- screenalytics code changes (owned by screenalytics TASK4)
- TRR-APP changes (none needed for this task)
- Supabase schema cleanup (separate task: TRR-Backend TASK4)

## Locked Contracts

### Migration DDL

Full DDL is in the source plan (`~/.claude/plans/screenalytics-supabase-unification.md`, Phase 1 section). Key contracts:

- `screenalytics.face_bank_images.person_id` references `core.people(id)` (not a mapping column)
- `screenalytics.video_asset_cast_candidates` uses composite PK `(video_asset_id, person_id)`, FK to `screenalytics.video_assets(id)`
- `screenalytics.runs.run_id` is `text PRIMARY KEY` (v1 convention), `ep_id` is an internal token
- All tables use `gen_random_uuid()` for UUID defaults
- RLS: `service_role` only, matching migration 0093 pattern

### Schema Divergence Resolution (v2 Tables)

Supabase v2 tables (from migration 0093) use different column names than screenalytics' local Docker copies:
- `screenalytics.video_assets`: Supabase PK is `id` (not `video_asset_id`)
- `screenalytics.runs_v2`: Supabase PK is `id` (not `run_id`), uses `manifest_key` (not `s3_manifest_key`)

TRR-Backend code must use the Supabase column names.

### Environment Variables

- `TRR_DB_URL` is the canonical connection string (Supabase direct Postgres)
- `SUPABASE_DB_URL` is a deprecated alias during transition

## Acceptance Criteria

1. Migrations 0102-0105 apply cleanly on staging Supabase instance.
2. No schema drift conflicts (preflight checks pass or reconciliation migrations written).
3. RLS policies match existing screenalytics table pattern from migration 0093.
4. TRR-Backend integration files (`repositories/screenalytics_runs.py`) use correct v2 column names (`id`, `manifest_key`).
5. Existing TRR-Backend tests pass with no regressions.
6. Cross-collab docs are synchronized across repos.
