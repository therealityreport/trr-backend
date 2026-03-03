#!/usr/bin/env bash
set -euo pipefail

# Starts a persistent social worker pool tuned for full sync + mirror workloads.
# Usage:
#   SOCIAL_QUEUE_ENABLED=true ./scripts/socials/start_worker_pool.sh
# Optional env knobs:
#   SOCIAL_WORKER_POOL_POSTS=6
#   SOCIAL_WORKER_POOL_COMMENTS=8
#   SOCIAL_WORKER_POOL_MEDIA_MIRROR=3
#   SOCIAL_WORKER_POOL_COMMENT_MEDIA_MIRROR=2
#   SOCIAL_WORKER_POOL_INTERVAL_SEC=2
#   SOCIAL_DB_UPSERT_BATCH_SIZE_COMMENTS=200
#   SOCIAL_DB_UPSERT_BATCH_SIZE_POSTS=50
#   PYTHON_BIN=python

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

PYTHON_BIN="${PYTHON_BIN:-python}"
POSTS_WORKERS="${SOCIAL_WORKER_POOL_POSTS:-6}"
COMMENTS_WORKERS="${SOCIAL_WORKER_POOL_COMMENTS:-8}"
MEDIA_MIRROR_WORKERS="${SOCIAL_WORKER_POOL_MEDIA_MIRROR:-3}"
COMMENT_MEDIA_MIRROR_WORKERS="${SOCIAL_WORKER_POOL_COMMENT_MEDIA_MIRROR:-2}"
WORKER_INTERVAL="${SOCIAL_WORKER_POOL_INTERVAL_SEC:-2}"

normalize_count() {
  local raw="${1:-0}"
  if [[ ! "$raw" =~ ^[0-9]+$ ]]; then
    echo 0
    return
  fi
  echo "$raw"
}

POSTS_WORKERS="$(normalize_count "$POSTS_WORKERS")"
COMMENTS_WORKERS="$(normalize_count "$COMMENTS_WORKERS")"
MEDIA_MIRROR_WORKERS="$(normalize_count "$MEDIA_MIRROR_WORKERS")"
COMMENT_MEDIA_MIRROR_WORKERS="$(normalize_count "$COMMENT_MEDIA_MIRROR_WORKERS")"

if [[ "$POSTS_WORKERS" -eq 0 && "$COMMENTS_WORKERS" -eq 0 && "$MEDIA_MIRROR_WORKERS" -eq 0 && "$COMMENT_MEDIA_MIRROR_WORKERS" -eq 0 ]]; then
  echo "[social-worker-pool] nothing to start (all worker pools are 0)"
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

if [[ "$POSTS_WORKERS" -gt 0 ]]; then
  start_worker "posts" --stage posts --parallel "$POSTS_WORKERS" --interval "$WORKER_INTERVAL"
fi

if [[ "$COMMENTS_WORKERS" -gt 0 ]]; then
  start_worker "comments" --stage comments --parallel "$COMMENTS_WORKERS" --interval "$WORKER_INTERVAL"
fi

if [[ "$MEDIA_MIRROR_WORKERS" -gt 0 ]]; then
  start_worker "media_mirror" --stage media_mirror --parallel "$MEDIA_MIRROR_WORKERS" --interval "$WORKER_INTERVAL"
fi

if [[ "$COMMENT_MEDIA_MIRROR_WORKERS" -gt 0 ]]; then
  start_worker "comment_media_mirror" --stage comment_media_mirror --parallel "$COMMENT_MEDIA_MIRROR_WORKERS" --interval "$WORKER_INTERVAL"
fi

echo "[social-worker-pool] started (posts=${POSTS_WORKERS}, comments=${COMMENTS_WORKERS}, media_mirror=${MEDIA_MIRROR_WORKERS}, comment_media_mirror=${COMMENT_MEDIA_MIRROR_WORKERS})"
wait
