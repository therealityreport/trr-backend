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

UVICORN_ARGS=(
  api.main:app
  --port "${TRR_BACKEND_PORT:-8000}"
  --timeout-graceful-shutdown "$GRACEFUL_SHUTDOWN_SECONDS"
)
BACKEND_RELOAD_MODE="${TRR_BACKEND_RELOAD:-1}"
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
