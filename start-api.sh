#!/usr/bin/env bash
set -euo pipefail

# Start TRR Backend API
# Run this once, leave it running for all local consumers (TRR-APP and backend-owned admin flows).

cd "$(dirname "$0")"

if [[ -f ".venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source ".venv/bin/activate"
elif ! command -v uvicorn >/dev/null 2>&1; then
  echo "[trr-backend] ERROR: missing .venv and uvicorn is not available on PATH." >&2
  echo "[trr-backend] ERROR: run (from workspace root) make bootstrap or install runtime dependencies." >&2
  exit 1
else
  echo "[trr-backend] WARNING: .venv not found; using uvicorn from PATH." >&2
fi

GRACEFUL_SHUTDOWN_SECONDS="${TRR_BACKEND_GRACEFUL_SHUTDOWN_SECONDS:-10}"
if ! [[ "$GRACEFUL_SHUTDOWN_SECONDS" =~ ^[1-9][0-9]*$ ]]; then
  echo "[trr-backend] WARNING: invalid TRR_BACKEND_GRACEFUL_SHUTDOWN_SECONDS='${GRACEFUL_SHUTDOWN_SECONDS}', using default 10." >&2
  GRACEFUL_SHUTDOWN_SECONDS="10"
fi

TRR_BACKEND_HOST="${TRR_BACKEND_HOST:-127.0.0.1}"
if [[ -z "$TRR_BACKEND_HOST" ]]; then
  echo "[trr-backend] WARNING: empty TRR_BACKEND_HOST, using default 127.0.0.1." >&2
  TRR_BACKEND_HOST="127.0.0.1"
fi

TRR_BACKEND_PORT="${TRR_BACKEND_PORT:-${PORT:-8000}}"
if ! [[ "$TRR_BACKEND_PORT" =~ ^[1-9][0-9]*$ ]]; then
  echo "[trr-backend] WARNING: invalid TRR_BACKEND_PORT='${TRR_BACKEND_PORT}', using default 8000." >&2
  TRR_BACKEND_PORT="8000"
fi

TRR_BACKEND_WORKERS="${TRR_BACKEND_WORKERS:-1}"
if ! [[ "$TRR_BACKEND_WORKERS" =~ ^[1-9][0-9]*$ ]]; then
  echo "[trr-backend] WARNING: invalid TRR_BACKEND_WORKERS='${TRR_BACKEND_WORKERS}', using default 1." >&2
  TRR_BACKEND_WORKERS="1"
fi

TRR_BACKEND_REQUIRE_REDIS_FOR_MULTI_WORKER="${TRR_BACKEND_REQUIRE_REDIS_FOR_MULTI_WORKER:-1}"
if ! [[ "$TRR_BACKEND_REQUIRE_REDIS_FOR_MULTI_WORKER" =~ ^[01]$ ]]; then
  echo "[trr-backend] WARNING: invalid TRR_BACKEND_REQUIRE_REDIS_FOR_MULTI_WORKER='${TRR_BACKEND_REQUIRE_REDIS_FOR_MULTI_WORKER}', using 1." >&2
  TRR_BACKEND_REQUIRE_REDIS_FOR_MULTI_WORKER="1"
fi

BACKEND_RELOAD_MODE="${TRR_BACKEND_RELOAD:-1}"

is_local_or_dev_runtime() {
  if [[ -n "${CI:-}" || -n "${GITHUB_ACTIONS:-}" ]]; then
    return 0
  fi

  local value normalized_value
  for value in "${APP_ENV:-}" "${ENVIRONMENT:-}" "${TRR_ENV:-}" "${TRR_ENVIRONMENT:-}" "${PYTHON_ENV:-}"; do
    normalized_value="$(printf '%s' "$value" | tr '[:upper:]' '[:lower:]')"
    case "$normalized_value" in
      local|dev|development|test)
        return 0
        ;;
    esac
  done

  local local_dev="${TRR_LOCAL_DEV:-}"
  local_dev="$(printf '%s' "$local_dev" | tr '[:upper:]' '[:lower:]')"
  case "$local_dev" in
    1|true|yes|on)
      return 0
      ;;
  esac

  return 1
}

EFFECTIVE_TRR_BACKEND_WORKERS="$TRR_BACKEND_WORKERS"
if [[ "$TRR_BACKEND_WORKERS" -gt 1 && "$TRR_BACKEND_REQUIRE_REDIS_FOR_MULTI_WORKER" == "1" ]]; then
  if [[ -z "${REDIS_URL:-}" ]]; then
    if is_local_or_dev_runtime; then
      echo "[trr-backend] WARNING: TRR_BACKEND_WORKERS=${TRR_BACKEND_WORKERS} requested, but REDIS_URL is not set."
      echo "[trr-backend] WARNING: forcing single-worker mode (TRR_BACKEND_WORKERS=1) to preserve realtime safety."
      EFFECTIVE_TRR_BACKEND_WORKERS="1"
    else
      echo "[trr-backend] ERROR: TRR_BACKEND_WORKERS=${TRR_BACKEND_WORKERS} requires REDIS_URL for deployed multi-worker realtime." >&2
      echo "[trr-backend] ERROR: set REDIS_URL or lower TRR_BACKEND_WORKERS to 1." >&2
      exit 1
    fi
  fi
fi
if [[ "$BACKEND_RELOAD_MODE" != "0" && "$EFFECTIVE_TRR_BACKEND_WORKERS" -gt 1 ]]; then
  echo "[trr-backend] WARNING: reload mode does not support multi-worker in this launcher."
  echo "[trr-backend] WARNING: forcing single-worker mode (TRR_BACKEND_WORKERS=1)."
  EFFECTIVE_TRR_BACKEND_WORKERS="1"
fi

UVICORN_ARGS=(
  api.main:app
  --host "$TRR_BACKEND_HOST"
  --port "$TRR_BACKEND_PORT"
  --timeout-graceful-shutdown "$GRACEFUL_SHUTDOWN_SECONDS"
)

# For local development, let repo-local .env provide backend-specific defaults
# that the workspace launcher does not export globally.
if [[ -f ".env" ]]; then
  UVICORN_ARGS+=(
    --env-file ".env"
  )
fi

if [[ "$EFFECTIVE_TRR_BACKEND_WORKERS" -gt 1 ]]; then
  UVICORN_ARGS+=(
    --workers "$EFFECTIVE_TRR_BACKEND_WORKERS"
  )
fi

if [[ "$BACKEND_RELOAD_MODE" != "0" ]]; then
  UVICORN_ARGS+=(
    --reload
    --reload-dir api
    --reload-dir trr_backend
    --reload-exclude ".logs/*"
    --reload-exclude ".venv/*"
    --reload-exclude "tests/*"
    --reload-exclude "scripts/*"
    --reload-exclude "supabase/*"
  )
fi

if [[ "$BACKEND_RELOAD_MODE" == "0" ]]; then
  echo "[trr-backend] starting in non-reload mode"
else
  echo "[trr-backend] starting in reload mode"
fi

exec uvicorn "${UVICORN_ARGS[@]}"
