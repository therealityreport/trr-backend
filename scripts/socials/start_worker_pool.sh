#!/usr/bin/env bash
set -euo pipefail

# Starts a persistent social worker pool tuned for full sync + mirror workloads.
# Usage:
#   SOCIAL_QUEUE_ENABLED=true ./scripts/socials/start_worker_pool.sh
# Optional env knobs:
#   SOCIAL_WORKER_POOL_GENERAL=4
#   SOCIAL_WORKER_POOL_MEDIA_MIRROR=2
#   SOCIAL_WORKER_POOL_INTERVAL_SEC=2
#   PYTHON_BIN=python

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

PYTHON_BIN="${PYTHON_BIN:-python}"
GENERAL_WORKERS="${SOCIAL_WORKER_POOL_GENERAL:-4}"
MEDIA_MIRROR_WORKERS="${SOCIAL_WORKER_POOL_MEDIA_MIRROR:-2}"
WORKER_INTERVAL="${SOCIAL_WORKER_POOL_INTERVAL_SEC:-2}"

normalize_count() {
  local raw="${1:-0}"
  if [[ ! "$raw" =~ ^[0-9]+$ ]]; then
    echo 0
    return
  fi
  echo "$raw"
}

GENERAL_WORKERS="$(normalize_count "$GENERAL_WORKERS")"
MEDIA_MIRROR_WORKERS="$(normalize_count "$MEDIA_MIRROR_WORKERS")"

if [[ "$GENERAL_WORKERS" -eq 0 && "$MEDIA_MIRROR_WORKERS" -eq 0 ]]; then
  echo "[social-worker-pool] nothing to start (both worker pools are 0)"
  exit 1
fi

if [[ "${SOCIAL_QUEUE_ENABLED:-}" == "" ]]; then
  export SOCIAL_QUEUE_ENABLED=true
fi

declare -a PIDS=()

start_worker() {
  local label="$1"
  shift
  echo "[social-worker-pool] starting ${label}: $*"
  "$PYTHON_BIN" -m scripts.socials.worker "$@" &
  PIDS+=("$!")
}

stop_all() {
  if [[ "${#PIDS[@]}" -eq 0 ]]; then
    return
  fi
  echo "[social-worker-pool] stopping workers..."
  for pid in "${PIDS[@]}"; do
    kill -TERM "$pid" >/dev/null 2>&1 || true
  done
  wait || true
}

trap stop_all EXIT INT TERM

if [[ "$GENERAL_WORKERS" -gt 0 ]]; then
  start_worker "general" --parallel "$GENERAL_WORKERS" --interval "$WORKER_INTERVAL"
fi

if [[ "$MEDIA_MIRROR_WORKERS" -gt 0 ]]; then
  start_worker "media_mirror" --stage media_mirror --parallel "$MEDIA_MIRROR_WORKERS" --interval "$WORKER_INTERVAL"
fi

echo "[social-worker-pool] started (general=${GENERAL_WORKERS}, media_mirror=${MEDIA_MIRROR_WORKERS})"
wait
