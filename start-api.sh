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

exec uvicorn api.main:app --reload --port "${TRR_BACKEND_PORT:-8000}"
