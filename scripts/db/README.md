# Database Scripts

This directory contains scripts for database maintenance, verification, and migrations.

## Quick Start

```bash
# Run any SQL script safely (auto-resolves DB URL)
./scripts/db/run_sql.sh scripts/db/verify_pre_0033_cleanup.sql

# Run an ad-hoc query
./scripts/db/run_sql.sh -c "SELECT count(*) FROM core.shows;"
```

## Files

| File | Purpose |
|------|---------|
| `run_sql.sh` | Safe SQL runner with auto DB URL resolution |
| `guard_core_schema.sql` | Abort script if `core` schema missing |
| `verify_pre_0033_cleanup.sql` | Pre-migration duplicate checks |
| `run_fk_index_inventory.py` | Freeze wave inventory YAML from live FK metadata |
| `run_fk_index_observer.py` | Capture baseline / snapshot CSVs for concurrent index rollout |
| `build_fk_index_wave_artifacts.py` | Generate forward SQL, rollback SQL, and wave status notes from frozen inventory |

---

## Database Connection

### How DB URL is Resolved

The tools in this directory resolve the database URL in this order:

1. **`TRR_DB_URL`** - Canonical runtime Postgres URL
2. **`TRR_DB_FALLBACK_URL`** - Explicit break-glass fallback
3. **`SUPABASE_DB_URL` / `DATABASE_URL`** - Deprecated compatibility inputs only
4. **`supabase status`** - Falls back to local Supabase instance (dev only)

### Shared-Schema Ownership

Backend owns shared-schema SQL in this workspace.

- Canonical migrations for shared database surfaces belong in `TRR-Backend`, especially `firebase_surveys.*`, `admin.*`, grants, RLS, and anything that depends on backend-owned schemas such as `core.*`.
- `TRR-APP/apps/web/scripts/run-migrations.mjs` now defaults to app-local migrations only.
- The app runner still exposes a transitional opt-in for older environments that have not finished porting shared-schema SQL out of `TRR-APP`, but that path is compatibility-only and should not be treated as the long-term owner.

### FK Index Hardening Workflow

- Freeze wave inventory:
  - `PYTHONPATH=. ./.venv/bin/python -m scripts.db.run_fk_index_inventory --wave wave-1 --output docs/db/fk-index-hardening/wave-1-inventory.yml`
  - `PYTHONPATH=. ./.venv/bin/python -m scripts.db.run_fk_index_inventory --wave wave-2 --output docs/db/fk-index-hardening/wave-2-inventory.yml`
- Build wave artifacts from the frozen inventory:
  - `PYTHONPATH=. ./.venv/bin/python -m scripts.db.build_fk_index_wave_artifacts --wave wave-1`
  - `PYTHONPATH=. ./.venv/bin/python -m scripts.db.build_fk_index_wave_artifacts --wave wave-2`
- Capture observer baseline once direct connectivity is fixed:
  - `PYTHONPATH=. ./.venv/bin/python -m scripts.db.run_fk_index_observer baseline --wave wave-1 --output docs/db/fk-index-hardening/evidence/wave-1/baseline.csv`
  - `PYTHONPATH=. ./.venv/bin/python -m scripts.db.run_fk_index_observer baseline --wave wave-2 --output docs/db/fk-index-hardening/evidence/wave-2/baseline.csv`

#### Apply-session PGAPPNAME contract

Forward SQL files (`docs/db/fk-index-hardening/wave-{1,2}-forward.sql`) begin with a `DO $pre$` guard that raises an exception unless the session's `application_name` matches `fk-index-<wave>-apply`.

Before running the apply, set `PGAPPNAME`:

```bash
export PGAPPNAME=fk-index-wave-1-apply
psql "$TRR_DB_URL" -f docs/db/fk-index-hardening/wave-1-forward.sql
```

(Substitute `fk-index-wave-2-apply` for Wave 2.)

This contract enables observer attribution via `pg_stat_activity.application_name` (captured by `fk_index_observer_snapshot.sql`) and acts as a fail-safe against pooler-rewritten connections. The guard is intentionally absent from rollback SQL so incident-response sessions aren't blocked.

### For Local Development

Start local Supabase and the tools will auto-detect:

```bash
supabase start
./scripts/db/run_sql.sh scripts/db/verify_pre_0033_cleanup.sql
```

Or export explicitly:

```bash
export TRR_DB_URL="$(supabase status --output env | grep '^DB_URL=' | cut -d= -f2-)"
```

### For Remote/Production

Set `TRR_DB_URL` explicitly:

```bash
# From Supabase Dashboard → Settings → Database → Connection string (URI)
export TRR_DB_URL='postgresql://postgres.<project>:<password>@<host>:5432/postgres'

./scripts/db/run_sql.sh scripts/db/verify_pre_0033_cleanup.sql
```

### Verifying Your Connection

Before running destructive migrations, verify you're on the right database:

```bash
# Quick check: should return show count
./scripts/db/run_sql.sh -c "SELECT count(*) FROM core.shows;"

# Full verification: all queries should return 0 rows
./scripts/db/run_sql.sh scripts/db/verify_pre_0033_cleanup.sql
```

---

## Core Schema Guard

All scripts in this directory include `guard_core_schema.sql` which:

- **Aborts immediately** if `core` schema doesn't exist
- **Prevents damage** to wrong databases (e.g., local Postgres without TRR schema)
- **Shows clear error** explaining how to fix

Example error when connected to wrong database:

```
ERROR:
╔══════════════════════════════════════════════════════════════════════╗
║  ERROR: Schema "core" does not exist!                                ║
╠══════════════════════════════════════════════════════════════════════╣
║  You are connected to the WRONG database.                            ║
║  Check your environment:                                             ║
║    - TRR_DB_URL should point to your runtime Postgres database      ║
║    - For local dev: run `supabase start` first                      ║
╚══════════════════════════════════════════════════════════════════════╝
```

### Using the Guard in Your Scripts

Include at the top of any SQL script:

```sql
\i scripts/db/guard_core_schema.sql

-- Your queries here...
SELECT * FROM core.shows;
```

---

## Pre-Migration Verification

Before applying migration 0033 (JSONB cleanup), run verification:

```bash
./scripts/db/run_sql.sh scripts/db/verify_pre_0033_cleanup.sql
```

This checks for:
- Duplicate IMDb IDs in `core.shows`
- Duplicate TMDb IDs in `core.shows`
- Duplicate image identities in `core.show_images`

**All queries must return 0 rows** before applying the cleanup migration.

---

## Migration History Reconciliation

When Supabase shows "remote migration versions not found locally":

### Diagnosing

```bash
# Check what Supabase thinks is applied
supabase db remote commit

# Compare with local files
ls supabase/migrations/

# Query migration table directly
./scripts/db/run_sql.sh -c "SELECT * FROM supabase_migrations.schema_migrations ORDER BY version;"
```

### Repair Options

1. **Restore missing files** from git:
   ```bash
   git log --all --full-history -- "supabase/migrations/NNNN_*.sql"
   git checkout <commit> -- supabase/migrations/NNNN_*.sql
   ```

2. **Remove from history** (if migration was never actually applied):
   ```bash
   ./scripts/db/run_sql.sh -c "DELETE FROM supabase_migrations.schema_migrations WHERE version = 'NNNN';"
   ```

3. **Create stub file** (if migration was applied differently):
   ```bash
   echo "-- Applied via different process" > supabase/migrations/NNNN_stub.sql
   ```

### Verify Repair

```bash
supabase db diff          # Should show no differences
supabase db push --dry-run  # Should show no pending migrations
```

---

## Python Utilities

For Python scripts, use the connection module:

```python
from trr_backend.db.connection import (
    resolve_database_url,
    validate_supabase_connection,
    DatabaseConnectionError,
)

# Get DB URL (same resolution order as run_sql.sh)
try:
    db_url = resolve_database_url()
except DatabaseConnectionError as e:
    print(f"No database configured: {e}")
    sys.exit(1)

# Validate schema exists
validate_supabase_connection(db_url)
```

For Supabase client operations:

```python
from trr_backend.db.preflight import assert_core_schema_exists, DatabasePreflightError

try:
    assert_core_schema_exists(db)
except DatabasePreflightError as e:
    print(f"Wrong database: {e}")
    sys.exit(1)
```

---

## Environment Variables Reference

| Variable | Purpose | When to Use |
|----------|---------|-------------|
| `TRR_DB_URL` | Canonical runtime Postgres connection | Runtime SQL helpers and local workspace runs |
| `TRR_DB_FALLBACK_URL` | Explicit break-glass fallback URL | Emergency fallback only |
| `SUPABASE_DB_URL` | Deprecated compatibility alias | Transitional fallback only |
| `DATABASE_URL` | Tooling-only Postgres connection | Third-party tools that explicitly require this name |
| `SUPABASE_URL` | Supabase REST API URL | Python SDK, not psql |
| `SUPABASE_SERVICE_ROLE_KEY` | Service role key (bypasses RLS) | Python SDK admin ops |

### Local Development Setup

```bash
# Start local Supabase
supabase start

# For psql/migrations (auto-detected by run_sql.sh)
export TRR_DB_URL="$(supabase status --output env | grep '^DB_URL=' | cut -d= -f2-)"

# For Python SDK
export SUPABASE_URL=http://localhost:54321
export SUPABASE_SERVICE_ROLE_KEY=$(supabase status -o json | jq -r '.DB.SERVICE_KEY')
```

### Production Setup

```bash
# From Supabase Dashboard → Settings → Database → Connection string
export TRR_DB_URL='postgresql://postgres.<project>:<password>@<host>:5432/postgres'

# Only for tooling that explicitly requires DATABASE_URL
export DATABASE_URL="$TRR_DB_URL"

# From Supabase Dashboard → Settings → API
export SUPABASE_URL='https://<project>.supabase.co'
export SUPABASE_SERVICE_ROLE_KEY='eyJ...'
```
