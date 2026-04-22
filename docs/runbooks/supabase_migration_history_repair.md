# Supabase Migration History Drift Repair

## Scope

> This runbook applies only to `TRR-Backend/supabase/migrations`. The TRR-APP migration runner is app-local and atomic; shared-schema writes from TRR-APP are not permitted. Manual `psql` history work on shared schema must be performed from this repo.

Use this runbook when `supabase db push` or `supabase migration list` fails because remote migration versions do not match local files.

Example failure:

- `Remote migration versions not found in local migrations directory`
- Suggested by CLI: `supabase migration repair --status reverted <version>`

## Symptoms

1. `supabase db push --db-url "${TRR_DB_URL:-$TRR_DB_FALLBACK_URL}"` fails even though SQL changes are additive.
2. Remote migration history (`supabase_migrations.schema_migrations`) contains versions absent from `supabase/migrations/`.
3. Teams are forced to apply SQL manually with `psql` to unblock urgent rollout work.

## Safety Rules

1. Do not delete rows from `supabase_migrations.schema_migrations`.
2. Do not edit already-applied migration SQL files.
3. Prefer explicit repair entries over rewriting history.
4. Snapshot history before any repair command.

## Procedure

### 1) Capture local and remote migration inventories

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
set -a && source .env && set +a

ls -1 supabase/migrations | sed 's/_.*//' > /tmp/local_migrations.txt
psql "${TRR_DB_URL:-$TRR_DB_FALLBACK_URL}" -Atc \
  "select version from supabase_migrations.schema_migrations order by version" \
  > /tmp/remote_migrations.txt

echo "Local-only:"
comm -23 /tmp/local_migrations.txt /tmp/remote_migrations.txt || true
echo "Remote-only:"
comm -13 /tmp/local_migrations.txt /tmp/remote_migrations.txt || true
```

### 2) Confirm whether remote-only versions are legitimate

1. Check Git history/PRs for the missing migration file.
2. If it existed but was removed locally by mistake, restore it from history.
3. If it was an out-of-band DB change, document it in handoff before repair.

### 3) Repair migration history state

Use Supabase repair commands to align remote history with intentional repo state:

```bash
supabase migration repair --status reverted <remote_only_version> --db-url "${TRR_DB_URL:-$TRR_DB_FALLBACK_URL}"
```

If repair fails with prepared-statement errors on pooled connections (for example
`prepared statement "lrupsc_1_0" already exists`), retry against pooler port
`5432` instead of `6543`:

```bash
ALT_DB_URL="$(python - <<'PY'
import os, urllib.parse
base_url = os.environ.get("TRR_DB_URL") or os.environ["TRR_DB_FALLBACK_URL"]
u = urllib.parse.urlparse(base_url)
user = urllib.parse.quote(urllib.parse.unquote(u.username or ""), safe="")
pwd = urllib.parse.quote(urllib.parse.unquote(u.password or ""), safe="")
print(f"postgresql://{user}:{pwd}@aws-1-us-east-1.pooler.supabase.com:5432/postgres")
PY
)"

supabase migration repair --status reverted <remote_only_version> --db-url "$ALT_DB_URL"
```

If needed, mark a known-applied local migration as applied:

```bash
supabase migration repair --status applied <local_version> --db-url "${TRR_DB_URL:-$TRR_DB_FALLBACK_URL}"
```

### 4) Re-validate CLI migration operations

```bash
supabase migration list --db-url "${TRR_DB_URL:-$TRR_DB_FALLBACK_URL}"
supabase db push --db-url "${TRR_DB_URL:-$TRR_DB_FALLBACK_URL}"
```

Expected: both commands complete without remote/local mismatch errors.

## Emergency Path (when rollout is blocked)

If production rollout is blocked and repair cannot be completed immediately:

1. Apply required migration SQL directly with `psql`.
2. Insert corresponding version row into `supabase_migrations.schema_migrations`.
3. Open a follow-up repair task to normalize history and restore `supabase db push` workflow.

This path is temporary and must be followed by full history repair.
