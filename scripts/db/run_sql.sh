#!/usr/bin/env bash
# =============================================================================
# Safe SQL Runner for TRR Backend
# =============================================================================
# Resolves database URL from environment, local .env, or local Supabase, then runs SQL.
# Includes guardrails to prevent running against the wrong database.
#
# Usage:
#   ./scripts/db/run_sql.sh scripts/db/verify_pre_0033_cleanup.sql
#   ./scripts/db/run_sql.sh -c "SELECT count(*) FROM core.shows;"
#
# Environment variables (checked in order):
#   TRR_DB_SESSION_URL  - Preferred Supabase session-pooler URL
#   TRR_DB_URL          - Compatibility runtime database URL
#   TRR_DB_FALLBACK_URL - Explicit fallback only
#   DATABASE_URL        - Tooling-only compatibility input
#   SUPABASE_DB_URL     - Deprecated compatibility input
#   (fallback)          - Local Supabase via `supabase status`
# =============================================================================

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color
DB_URL=""
DB_URL_SOURCE=""

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

read_dotenv_value() {
    local key="$1"
    local dotenv_path="${TRR_BACKEND_DOTENV_PATH:-.env}"

    if [[ ! -f "$dotenv_path" ]]; then
        return 1
    fi

    local line
    line=$(grep -E "^[[:space:]]*${key}=" "$dotenv_path" | tail -n 1 || true)
    if [[ -z "$line" ]]; then
        return 1
    fi

    local value="${line#*=}"
    value="${value%$'\r'}"
    if [[ "$value" == \"*\" && "$value" == *\" ]]; then
        value="${value#\"}"
        value="${value%\"}"
    elif [[ "$value" == \'*\' && "$value" == *\' ]]; then
        value="${value#\'}"
        value="${value%\'}"
    fi

    if [[ -z "$value" ]]; then
        return 1
    fi
    printf '%s\n' "$value"
}

use_configured_db_url() {
    local key="$1"
    local value="${!key:-}"
    if [[ -n "$value" ]]; then
        DB_URL="$value"
        DB_URL_SOURCE="$key"
        return 0
    fi

    if value=$(read_dotenv_value "$key"); then
        DB_URL="$value"
        DB_URL_SOURCE="$key (.env)"
        return 0
    fi

    return 1
}

# Resolve database URL
resolve_db_url() {
    # Priority 1: Preferred session-pooler URL
    if use_configured_db_url "TRR_DB_SESSION_URL"; then
        return 0
    fi

    # Priority 2: Compatibility runtime DB URL
    if use_configured_db_url "TRR_DB_URL"; then
        return 0
    fi

    # Priority 3: Explicit runtime fallback
    if use_configured_db_url "TRR_DB_FALLBACK_URL"; then
        return 0
    fi

    # Priority 4: Tooling-only compatibility input
    if use_configured_db_url "DATABASE_URL"; then
        return 0
    fi

    # Priority 5: Deprecated compatibility input
    if use_configured_db_url "SUPABASE_DB_URL"; then
        return 0
    fi

    # Priority 6: Local Supabase fallback
    if command -v supabase &>/dev/null; then
        local status_output
        if status_output=$(supabase status --output env 2>/dev/null); then
            local db_url
            db_url=$(echo "$status_output" | grep '^DB_URL=' | cut -d'=' -f2- | tr -d '"')
            if [[ -n "$db_url" ]]; then
                DB_URL="$db_url"
                DB_URL_SOURCE="supabase status (local)"
                return 0
            fi
        fi
    fi

    return 1
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
        echo "  TRR_DB_SESSION_URL  - Preferred Supabase session-pooler URL"
        echo "  TRR_DB_URL          - Compatibility runtime database URL"
        echo "  TRR_DB_FALLBACK_URL - Explicit fallback only"
        echo "  DATABASE_URL        - Tooling-only compatibility input"
        echo "  SUPABASE_DB_URL     - Deprecated compatibility input"
        echo "  (fallback)          - Local Supabase via 'supabase status'"
        exit 1
    fi

    # Resolve database URL
    if ! resolve_db_url; then
        error "No database URL configured.

For remote/production:
  Set TRR_DB_SESSION_URL or TRR_DB_URL to your Supabase session-pooler connection string.
  Example: export TRR_DB_SESSION_URL='postgresql://postgres.<project>:<password>@<host>:5432/postgres'

For local development:
  Start local Supabase: supabase start
  Or set DATABASE_URL only for tooling-specific local flows."
    fi

    local db_url="$DB_URL"
    local url_source="$DB_URL_SOURCE"
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
