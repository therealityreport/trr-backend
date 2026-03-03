#!/usr/bin/env bash
set -euo pipefail

# Start TRR Backend API
# Run this once, leave it running for all frontends (TRR-APP, screenalytics, etc.)

cd "$(dirname "$0")"

if [[ ! -f ".venv/bin/activate" ]]; then
  echo "[trr-backend] ERROR: missing .venv. Run: (from workspace root) make bootstrap" >&2
  exit 1
fi

# shellcheck disable=SC1091
source ".venv/bin/activate"

GRACEFUL_SHUTDOWN_SECONDS="${TRR_BACKEND_GRACEFUL_SHUTDOWN_SECONDS:-10}"
if ! [[ "$GRACEFUL_SHUTDOWN_SECONDS" =~ ^[1-9][0-9]*$ ]]; then
  echo "[trr-backend] WARNING: invalid TRR_BACKEND_GRACEFUL_SHUTDOWN_SECONDS='${GRACEFUL_SHUTDOWN_SECONDS}', using default 10." >&2
  GRACEFUL_SHUTDOWN_SECONDS="10"
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

EFFECTIVE_TRR_BACKEND_WORKERS="$TRR_BACKEND_WORKERS"
if [[ "$TRR_BACKEND_WORKERS" -gt 1 && "$TRR_BACKEND_REQUIRE_REDIS_FOR_MULTI_WORKER" == "1" ]]; then
  if [[ -z "${REDIS_URL:-}" ]]; then
    echo "[trr-backend] WARNING: TRR_BACKEND_WORKERS=${TRR_BACKEND_WORKERS} requested, but REDIS_URL is not set."
    echo "[trr-backend] WARNING: forcing single-worker mode (TRR_BACKEND_WORKERS=1) to preserve realtime safety."
    EFFECTIVE_TRR_BACKEND_WORKERS="1"
  fi
fi
if [[ "$BACKEND_RELOAD_MODE" != "0" && "$EFFECTIVE_TRR_BACKEND_WORKERS" -gt 1 ]]; then
  echo "[trr-backend] WARNING: reload mode does not support multi-worker in this launcher."
  echo "[trr-backend] WARNING: forcing single-worker mode (TRR_BACKEND_WORKERS=1)."
  EFFECTIVE_TRR_BACKEND_WORKERS="1"
fi

UVICORN_ARGS=(
  api.main:app
  --port "${TRR_BACKEND_PORT:-8000}"
  --timeout-graceful-shutdown "$GRACEFUL_SHUTDOWN_SECONDS"
)

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
