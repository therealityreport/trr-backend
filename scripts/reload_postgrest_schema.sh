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
#   - SUPABASE_DB_URL must be set in .env or environment
#   - psql must be installed
#
# See: docs/runbooks/postgrest_schema_cache.md

# Load .env if it exists
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi

# Check if SUPABASE_DB_URL is set
if [ -z "${SUPABASE_DB_URL:-}" ]; then
    echo "ERROR: SUPABASE_DB_URL is not set"
    echo ""
    echo "Set it in .env or export it:"
    echo "  export SUPABASE_DB_URL='postgresql://postgres:password@db.project.supabase.co:5432/postgres'"
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
psql "$SUPABASE_DB_URL" -f scripts/reload_postgrest_schema.sql

echo "✅ PostgREST schema cache reloaded successfully"
echo ""
echo "You can now call newly created RPC functions via Supabase client."
