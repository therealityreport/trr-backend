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

---

## Database Connection

### How DB URL is Resolved

The tools in this directory resolve the database URL in this order:

1. **`SUPABASE_DB_URL`** - Explicit Supabase database URL (recommended for production)
2. **`DATABASE_URL`** - Standard Postgres connection string
3. **`TRR_DB_URL`** - Legacy alias
4. **`supabase status`** - Falls back to local Supabase instance (dev only)

### For Local Development

Start local Supabase and the tools will auto-detect:

```bash
supabase start
./scripts/db/run_sql.sh scripts/db/verify_pre_0033_cleanup.sql
```

Or export explicitly:

```bash
export DATABASE_URL=$(supabase status --output env | grep DB_URL | cut -d= -f2)
```

### For Remote/Production

Set `SUPABASE_DB_URL` explicitly:

```bash
# From Supabase Dashboard → Settings → Database → Connection string (URI)
export SUPABASE_DB_URL='postgresql://postgres.<project>:<password>@<host>:5432/postgres'

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
║    - SUPABASE_DB_URL should point to your Supabase project          ║
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
| `SUPABASE_DB_URL` | Direct Postgres connection to Supabase | Production migrations |
| `DATABASE_URL` | Standard Postgres connection | General purpose |
| `TRR_DB_URL` | Legacy alias for DATABASE_URL | Backward compatibility |
| `SUPABASE_URL` | Supabase REST API URL | Python SDK, not psql |
| `SUPABASE_SERVICE_ROLE_KEY` | Service role key (bypasses RLS) | Python SDK admin ops |

### Local Development Setup

```bash
# Start local Supabase
supabase start

# For psql/migrations (auto-detected by run_sql.sh)
export DATABASE_URL=$(supabase status --output env | grep DB_URL | cut -d= -f2)

# For Python SDK
export SUPABASE_URL=http://localhost:54321
export SUPABASE_SERVICE_ROLE_KEY=$(supabase status -o json | jq -r '.DB.SERVICE_KEY')
```

### Production Setup

```bash
# From Supabase Dashboard → Settings → Database → Connection string
export SUPABASE_DB_URL='postgresql://postgres.<project>:<password>@<host>:5432/postgres'

# From Supabase Dashboard → Settings → API
export SUPABASE_URL='https://<project>.supabase.co'
export SUPABASE_SERVICE_ROLE_KEY='eyJ...'
```
