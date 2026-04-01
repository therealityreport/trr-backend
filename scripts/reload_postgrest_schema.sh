#!/usr/bin/env bash
set -euo pipefail

# Reload PostgREST schema cache
#
# Run this after applying migrations that add or modify database functions.
# PostgREST caches the database schema at startup and needs to be notified
# to reload when functions are added/changed.
#
# Usage:
#   ./scripts/reload_postgrest_schema.sh
#
# Prerequisites:
#   - TRR_DB_URL or TRR_DB_FALLBACK_URL must be set in .env or environment
#   - psql must be installed
#
# See: docs/runbooks/postgrest_schema_cache.md

# Load .env if it exists
if [ -f .env ]; then
    tmp_env_exports="$(mktemp)"
    python - <<'PY' > "$tmp_env_exports"
from pathlib import Path
import ast
import shlex

for raw_line in Path(".env").read_text().splitlines():
    line = raw_line.strip()
    if not line or line.startswith("#"):
        continue
    if line.startswith("export "):
        line = line[7:].strip()
    if "=" not in line:
        continue
    key, value = line.split("=", 1)
    key = key.strip()
    value = value.strip()
    if not key:
        continue
    if value[:1] in {"'", '"'} and value[-1:] == value[:1]:
        try:
            value = ast.literal_eval(value)
        except Exception:
            value = value[1:-1]
    print(f"export {key}={shlex.quote(value)}")
PY
    # shellcheck disable=SC1090
    . "$tmp_env_exports"
    rm -f "$tmp_env_exports"
fi

# Check if runtime DB URL is set
db_url="${TRR_DB_URL:-${TRR_DB_FALLBACK_URL:-}}"
if [ -z "${db_url}" ]; then
    echo "ERROR: TRR_DB_URL or TRR_DB_FALLBACK_URL is not set"
    echo ""
    echo "Set it in .env or export it:"
    echo "  export TRR_DB_URL='postgresql://postgres:password@aws-1-us-east-1.pooler.supabase.com:5432/postgres'"
    exit 1
fi

# Check if psql is available
if ! command -v psql &> /dev/null; then
    echo "ERROR: psql is not installed"
    echo ""
    echo "Install PostgreSQL client:"
    echo "  macOS: brew install postgresql"
    echo "  Ubuntu: sudo apt-get install postgresql-client"
    exit 1
fi

echo "Reloading PostgREST schema cache..."
psql "$db_url" -f scripts/reload_postgrest_schema.sql

echo "✅ PostgREST schema cache reloaded successfully"
echo ""
echo "You can now call newly created RPC functions via Supabase client."
