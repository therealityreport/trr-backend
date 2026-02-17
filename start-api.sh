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

UVICORN_ARGS=(api.main:app --port "${TRR_BACKEND_PORT:-8000}")
if [[ "${TRR_BACKEND_RELOAD:-1}" != "0" ]]; then
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

exec uvicorn "${UVICORN_ARGS[@]}"
