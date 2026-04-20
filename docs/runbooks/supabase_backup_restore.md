# Supabase Backup and Restore Runbook

Last updated: 2026-04-17
Owner: TRR-Backend

## Goal

Document the repo-managed recovery flow for the shared Supabase database:

- what this workspace assumes about backups and point-in-time recovery (PITR)
- how to capture a repo-side pre-restore snapshot
- how to run a restore drill against an isolated target
- how to verify the restored target before any app or backend cutover

This runbook is for operational follow-through. It does not replace Supabase's
platform restore controls.

## Ownership

- Primary owner: TRR-Backend maintainers own repo-side verification, migration
  parity, and database contract checks.
- Required platform operator: a human with Supabase dashboard restore access for
  the production project and the ability to create an isolated restore target.
- Downstream coordination: if a restore is promoted beyond a drill, both
  TRR-Backend and TRR-APP operators must update the canonical runtime DB envs
  (`TRR_DB_URL`, optional `TRR_DB_FALLBACK_URL`) together.

## PITR Assumptions

- Primary backup path is Supabase-managed backups plus PITR for the hosted
  production project.
- Restore work starts from a Supabase restore point and lands in an isolated
  branch or other disposable database target first. This matches the repo's
  existing preference for isolated remote validation over destructive replay on
  shared persistent databases.
- `TRR_DB_URL` is the canonical runtime Postgres URL for both TRR-Backend and
  TRR-APP. `TRR_DB_FALLBACK_URL` is the only intentional runtime fallback lane.
- Runtime app traffic stays on the Supavisor session pooler lane. Large backup
  or restore operations may require a tool-only connection string outside the
  normal runtime env contract, but do not change runtime code defaults to make
  that easier.
- There is no checked-in scheduler or retention automation in this repo for
  logical dumps. Any logical dump captured here is an operator-created artifact,
  stored outside the repo.

If any of those assumptions are false in the platform, stop and correct them
before treating this runbook as a recovery plan.

## Safety Rules

1. Never run a drill by restoring over the active production project.
2. Never point shared Render or Vercel runtimes at a restored target until repo
   verification passes.
3. Never commit backup artifacts or secret-bearing connection strings.
4. Keep backup artifacts under an external operator path such as
   `../artifacts/trr-backend/backups/`.
5. Verify target identity with repo SQL helpers before any write, replay, or
   migration command.
6. Do not edit existing migration files during recovery. If migration history is
   broken, use the dedicated migration repair runbook.

## When To Use This

- Before a high-risk schema rollout when PITR readiness needs a fresh drill.
- After operator error or destructive data loss in the hosted Supabase project.
- When an incident requires proving that a restore target can boot the backend
  and satisfy repo schema checks before cutover.

## Pre-Drill Checklist

- [ ] Supabase operator confirms PITR is enabled for the production project.
- [ ] Restore timestamp or incident timestamp is written down in UTC.
- [ ] An isolated restore target has been chosen (branch or disposable project).
- [ ] You have a local checkout at `/Users/thomashulihan/Projects/TRR/TRR-Backend`.
- [ ] `.env` is loaded locally and `TRR_DB_URL` points at the source database.
- [ ] You have disk space outside the repo for drill artifacts.
- [ ] You know who will approve any real cutover after the drill.

## 1) Capture Source Snapshot Before Restore

Run from the backend repo:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
set -a && source .env && set +a

STAMP="$(date +%Y%m%d-%H%M%S)"
ARTIFACT_DIR="../artifacts/trr-backend/backups/${STAMP}"
mkdir -p "$ARTIFACT_DIR"
```

Capture source identity and representative table counts:

```bash
./scripts/db/run_sql.sh -c "
select current_database() as database_name,
       current_user as db_user,
       now() as captured_at;
" | tee "$ARTIFACT_DIR/source-identity.txt"

./scripts/db/run_sql.sh -c "
select 'core.shows' as table_name, count(*) as row_count from core.shows
union all
select 'core.people', count(*) from core.people
union all
select 'core.seasons', count(*) from core.seasons
union all
select 'supabase_migrations.schema_migrations', count(*) from supabase_migrations.schema_migrations;
" | tee "$ARTIFACT_DIR/source-counts.txt"

psql "${TRR_DB_URL:-$TRR_DB_FALLBACK_URL}" -Atc "
select version
from supabase_migrations.schema_migrations
order by version desc
limit 10;
" > "$ARTIFACT_DIR/source-migrations-tail.txt"
```

Optional logical snapshot for extra operator confidence before a risky change:

```bash
supabase db dump \
  --db-url "${TRR_DB_URL:-$TRR_DB_FALLBACK_URL}" \
  --schema public,core,admin \
  --file "$ARTIFACT_DIR/prechange-schema.sql"

supabase db dump \
  --db-url "${TRR_DB_URL:-$TRR_DB_FALLBACK_URL}" \
  --data-only \
  --use-copy \
  --schema public,core,admin \
  --file "$ARTIFACT_DIR/prechange-data.sql"
```

Notes:

- The logical dump is optional. PITR is the primary rollback path.
- If the runtime session-pooler lane is too constrained for a large dump, use a
  tool-only database URL for the dump itself rather than widening runtime pools.

## 2) Perform The Supabase Restore Outside The Repo

Use the Supabase dashboard or approved platform workflow to restore the source
project to the chosen PITR timestamp into an isolated target.

Record these operator outputs in the same artifact directory:

- restore target name
- restore timestamp used
- new database URL location or secret reference
- new Supabase project or branch URL, if applicable

This repo does not own the platform restore button. The repo-owned work begins
once the isolated target exists.

## 3) Point Repo Verification At The Restored Target

Export the restored target as the canonical DB URL for this shell only:

```bash
export TRR_DB_URL="<restored target db url>"
unset TRR_DB_FALLBACK_URL
```

Confirm the repo is no longer pointed at the production source:

```bash
./scripts/db/run_sql.sh -c "
select current_database() as database_name,
       current_user as db_user,
       now() as verified_at;
"
```

If the result is not the restored target you expected, stop immediately.

## 4) Restore Verification Steps

Verify schema presence first:

```bash
./scripts/db/run_sql.sh -c "
select schema_name
from information_schema.schemata
where schema_name in ('public', 'core', 'admin')
order by schema_name;
"
```

Verify representative row counts against the source snapshot:

```bash
./scripts/db/run_sql.sh -c "
select 'core.shows' as table_name, count(*) as row_count from core.shows
union all
select 'core.people', count(*) from core.people
union all
select 'core.seasons', count(*) from core.seasons
union all
select 'supabase_migrations.schema_migrations', count(*) from supabase_migrations.schema_migrations;
"
```

Verify migration history is readable from the restored target:

```bash
supabase migration list --db-url "${TRR_DB_URL}"
```

Verify repo schema docs still match the restored target:

```bash
make schema-docs-check
```

Optional backend boot smoke before any cutover:

```bash
TRR_BACKEND_RELOAD=0 ./start-api.sh
```

In a second shell:

```bash
curl -fsS http://127.0.0.1:8000/health
curl -fsS http://127.0.0.1:8000/health/live
```

Stop the local backend after the smoke passes.

## Restore Drill Exit Criteria

- [ ] Restored target identity was verified before any validation commands.
- [ ] `public`, `core`, and `admin` schemas exist on the restored target.
- [ ] Representative row counts are acceptable against the source snapshot.
- [ ] `supabase migration list --db-url "${TRR_DB_URL}"` completed successfully.
- [ ] `make schema-docs-check` completed successfully.
- [ ] Optional backend `/health` smoke passed if cutover readiness was required.
- [ ] Artifact directory contains source counts, migration tail, and restore notes.

If any box stays unchecked, the drill is not complete.

## If This Becomes A Real Cutover

After the restored target passes verification:

1. Freeze risky writes while env changes are in progress.
2. Update runtime secrets so both TRR-Backend and TRR-APP use the restored
   target through `TRR_DB_URL` and optional `TRR_DB_FALLBACK_URL`.
3. Restart or redeploy Render and Vercel so the new DB envs are picked up.
4. Re-run the row-count and `/health` checks against the live runtime.
5. Record the incident timestamp, restore target, verifier, and follow-up tasks.

Do not partially cut over only one repo's runtime DB envs.

## Related Runbooks

- `docs/runbooks/supabase_migration_history_repair.md`
- `docs/runbooks/postgrest_schema_cache.md`
- `/Users/thomashulihan/Projects/TRR/docs/workspace/supabase-capacity-budget.md`
