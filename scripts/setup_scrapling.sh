#!/usr/bin/env bash
# Setup script for the Instagram comments Scrapling lane.
#
# Installs the Python package (via requirements.lock.txt) AND the browser
# runtime `StealthyFetcher` depends on (Patchright/Playwright Chromium in
# Scrapling 0.4.x). The second step is the one that's easy to miss: `pip
# install scrapling[fetchers]` does NOT ship the browser binaries. Without
# this step, the first live fetch crashes with "browser binary not found".
# Scrapling 0.4.9 also refreshed browsers/fingerprints, so force the asset
# refresh after dependency upgrades.
#
# Usage:
#   ./scripts/setup_scrapling.sh              # dev venv at ./.venv
#   PYTHON=/custom/python ./scripts/setup_scrapling.sh   # override
#
# Idempotent: safe to re-run.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PYTHON_BIN="${PYTHON:-${ROOT_DIR}/.venv/bin/python}"
SCRAPLING_BIN="${SCRAPLING:-${ROOT_DIR}/.venv/bin/scrapling}"

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "[setup_scrapling] ERROR: python not found at '$PYTHON_BIN'."
  echo "[setup_scrapling]  Create the venv first: python3.11 -m venv .venv"
  exit 1
fi

echo "[setup_scrapling] 1/3 — pip install from requirements.lock.txt"
"$PYTHON_BIN" -m pip install -r requirements.lock.txt

if [[ ! -x "$SCRAPLING_BIN" ]]; then
  echo "[setup_scrapling] ERROR: 'scrapling' CLI not found at '$SCRAPLING_BIN' after install."
  echo "[setup_scrapling]  Expected scrapling[fetchers] extras to include the CLI."
  exit 2
fi

echo "[setup_scrapling] 2/3 — scrapling install --force (browser runtime)"
"$SCRAPLING_BIN" install --force

echo "[setup_scrapling] 3/3 — smoke import"
"$PYTHON_BIN" -c "import scrapling; from scrapling.fetchers import StealthyFetcher; print(f'scrapling {scrapling.__version__} StealthyFetcher ok')"

echo "[setup_scrapling] done. Next: copy .env values from .env.example and run:"
echo "[setup_scrapling]   TRR_SOCIAL_INGEST_WORKER_ENABLED=1 \\"
echo "[setup_scrapling]   TRR_SOCIAL_INGEST_WORKER_COMMENTS_SCRAPLING=1 \\"
echo "[setup_scrapling]   ./scripts/start_remote_job_workers.sh"
