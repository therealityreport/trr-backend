#!/usr/bin/env bash
# =============================================================================
# Safe SQL Runner for TRR Backend
# =============================================================================
# Resolves database URL from environment or local Supabase, then runs SQL.
# Includes guardrails to prevent running against the wrong database.
#
# Usage:
#   ./scripts/db/run_sql.sh scripts/db/verify_pre_0033_cleanup.sql
#   ./scripts/db/run_sql.sh -c "SELECT count(*) FROM core.shows;"
#
# Environment variables (checked in order):
#   SUPABASE_DB_URL - Explicit Supabase database URL (recommended for prod)
#   DATABASE_URL    - Standard Postgres connection string
#   TRR_DB_URL      - Legacy alias
#   (fallback)      - Local Supabase via `supabase status`
# =============================================================================

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

error() {
    echo -e "${RED}ERROR:${NC} $1" >&2
    exit 1
}

warn() {
    echo -e "${YELLOW}WARNING:${NC} $1" >&2
}

info() {
    echo -e "${GREEN}INFO:${NC} $1"
}

# Resolve database URL
resolve_db_url() {
    # Priority 1: Explicit Supabase DB URL
    if [[ -n "${SUPABASE_DB_URL:-}" ]]; then
        echo "$SUPABASE_DB_URL"
        return 0
    fi

    # Priority 2: Standard DATABASE_URL
    if [[ -n "${DATABASE_URL:-}" ]]; then
        echo "$DATABASE_URL"
        return 0
    fi

    # Priority 3: Legacy TRR_DB_URL
    if [[ -n "${TRR_DB_URL:-}" ]]; then
        echo "$TRR_DB_URL"
        return 0
    fi

    # Priority 4: Local Supabase fallback
    if command -v supabase &>/dev/null; then
        local status_output
        if status_output=$(supabase status --output env 2>/dev/null); then
            local db_url
            db_url=$(echo "$status_output" | grep '^DB_URL=' | cut -d'=' -f2- | tr -d '"')
            if [[ -n "$db_url" ]]; then
                echo "$db_url"
                return 0
            fi
        fi
    fi

    return 1
}

# Get source of resolved URL for display
get_url_source() {
    if [[ -n "${SUPABASE_DB_URL:-}" ]]; then
        echo "SUPABASE_DB_URL"
    elif [[ -n "${DATABASE_URL:-}" ]]; then
        echo "DATABASE_URL"
    elif [[ -n "${TRR_DB_URL:-}" ]]; then
        echo "TRR_DB_URL"
    else
        echo "supabase status (local)"
    fi
}

# Mask password in URL for display
mask_url() {
    local url="$1"
    echo "$url" | sed -E 's/(postgresql:\/\/[^:]+:)[^@]+(@)/\1****\2/'
}

# Check if URL looks like Supabase
is_supabase_url() {
    local url="$1"
    [[ "$url" == *"supabase"* ]] || \
    [[ "$url" == *".supabase.co"* ]] || \
    [[ "$url" == *":54322"* ]] || \
    [[ "$url" == *"pooler.supabase.com"* ]]
}

# Main
main() {
    if [[ $# -eq 0 ]]; then
        echo "Usage: $0 <sql-file> | -c <sql-command>"
        echo ""
        echo "Examples:"
        echo "  $0 scripts/db/verify_pre_0033_cleanup.sql"
        echo "  $0 -c 'SELECT count(*) FROM core.shows;'"
        echo ""
        echo "Environment variables (checked in order):"
        echo "  SUPABASE_DB_URL - Explicit Supabase database URL"
        echo "  DATABASE_URL    - Standard Postgres connection string"
        echo "  TRR_DB_URL      - Legacy alias"
        echo "  (fallback)      - Local Supabase via 'supabase status'"
        exit 1
    fi

    # Resolve database URL
    local db_url
    if ! db_url=$(resolve_db_url); then
        error "No database URL configured.

For remote/production:
  Set SUPABASE_DB_URL to your Supabase direct connection string.
  Example: export SUPABASE_DB_URL='postgresql://postgres.<project>:<password>@<host>:5432/postgres'

For local development:
  Start local Supabase: supabase start
  Or set DATABASE_URL to your local Postgres connection string."
    fi

    local url_source
    url_source=$(get_url_source)
    local masked_url
    masked_url=$(mask_url "$db_url")

    info "Database URL resolved from: $url_source"
    info "Connection: $masked_url"

    # Warn if URL doesn't look like Supabase
    if ! is_supabase_url "$db_url"; then
        warn "URL does not appear to be a Supabase instance.
       Ensure 'core' schema exists before running migrations."
    fi

    echo ""

    # Run psql with the resolved URL
    if [[ "$1" == "-c" ]]; then
        shift
        psql "$db_url" -c "$*"
    else
        # Change to repo root so \i paths work correctly
        cd "$(dirname "$0")/../.."
        psql "$db_url" -f "$1"
    fi
}

main "$@"
