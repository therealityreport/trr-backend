#!/usr/bin/env bash
set -euo pipefail

# Starts a persistent social worker pool tuned for full sync + mirror workloads.
# Usage:
#   SOCIAL_QUEUE_ENABLED=true ./scripts/socials/start_worker_pool.sh
# Optional env knobs:
#   SOCIAL_WORKER_MIN_STAGE_RUNNERS=1
#   SOCIAL_WORKER_ALLOW_STAGE_DISABLE=1
#   SOCIAL_WORKER_POOL_POSTS=1
#   SOCIAL_WORKER_POOL_COMMENTS=1
#   SOCIAL_WORKER_POOL_SHARED_ACCOUNT_POSTS=1
#   SOCIAL_WORKER_POOL_POST_CLASSIFY=1
#   SOCIAL_WORKER_POOL_SEASON_MATERIALIZE=1
#   SOCIAL_WORKER_POOL_ANALYTICS_REFRESH=1
#   SOCIAL_WORKER_POOL_MEDIA_MIRROR=0
#   SOCIAL_WORKER_POOL_COMMENT_MEDIA_MIRROR=0
#   SOCIAL_WORKER_POOL_INTERVAL_SEC=3
#   SOCIAL_DB_UPSERT_BATCH_SIZE_COMMENTS=200
#   SOCIAL_DB_UPSERT_BATCH_SIZE_POSTS=50
#   PYTHON_BIN=python

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

PYTHON_BIN="${PYTHON_BIN:-python}"
MIN_STAGE_RUNNERS="${SOCIAL_WORKER_MIN_STAGE_RUNNERS:-1}"
ALLOW_STAGE_DISABLE="${SOCIAL_WORKER_ALLOW_STAGE_DISABLE:-1}"
POSTS_WORKERS="${SOCIAL_WORKER_POOL_POSTS:-1}"
COMMENTS_WORKERS="${SOCIAL_WORKER_POOL_COMMENTS:-1}"
SHARED_ACCOUNT_POSTS_WORKERS="${SOCIAL_WORKER_POOL_SHARED_ACCOUNT_POSTS:-1}"
POST_CLASSIFY_WORKERS="${SOCIAL_WORKER_POOL_POST_CLASSIFY:-1}"
SEASON_MATERIALIZE_WORKERS="${SOCIAL_WORKER_POOL_SEASON_MATERIALIZE:-1}"
ANALYTICS_REFRESH_WORKERS="${SOCIAL_WORKER_POOL_ANALYTICS_REFRESH:-1}"
MEDIA_MIRROR_WORKERS="${SOCIAL_WORKER_POOL_MEDIA_MIRROR:-0}"
COMMENT_MEDIA_MIRROR_WORKERS="${SOCIAL_WORKER_POOL_COMMENT_MEDIA_MIRROR:-0}"
WORKER_INTERVAL="${SOCIAL_WORKER_POOL_INTERVAL_SEC:-3}"

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
SHARED_ACCOUNT_POSTS_WORKERS="$(normalize_count "$SHARED_ACCOUNT_POSTS_WORKERS")"
POST_CLASSIFY_WORKERS="$(normalize_count "$POST_CLASSIFY_WORKERS")"
SEASON_MATERIALIZE_WORKERS="$(normalize_count "$SEASON_MATERIALIZE_WORKERS")"
ANALYTICS_REFRESH_WORKERS="$(normalize_count "$ANALYTICS_REFRESH_WORKERS")"
MEDIA_MIRROR_WORKERS="$(normalize_count "$MEDIA_MIRROR_WORKERS")"
COMMENT_MEDIA_MIRROR_WORKERS="$(normalize_count "$COMMENT_MEDIA_MIRROR_WORKERS")"
MIN_STAGE_RUNNERS="$(normalize_count "$MIN_STAGE_RUNNERS")"
if [[ "$MIN_STAGE_RUNNERS" -lt 1 ]]; then
  MIN_STAGE_RUNNERS=1
fi

if [[ "$ALLOW_STAGE_DISABLE" != "1" ]]; then
  ALLOW_STAGE_DISABLE="0"
fi

apply_stage_floor() {
  local value="$1"
  if [[ "$value" -eq 0 ]]; then
    if [[ "$ALLOW_STAGE_DISABLE" -eq 1 ]]; then
      echo 0
      return
    fi
    echo "$MIN_STAGE_RUNNERS"
    return
  fi
  if [[ "$value" -lt "$MIN_STAGE_RUNNERS" ]]; then
    echo "$MIN_STAGE_RUNNERS"
    return
  fi
  echo "$value"
}

POSTS_WORKERS="$(apply_stage_floor "$POSTS_WORKERS")"
COMMENTS_WORKERS="$(apply_stage_floor "$COMMENTS_WORKERS")"
SHARED_ACCOUNT_POSTS_WORKERS="$(apply_stage_floor "$SHARED_ACCOUNT_POSTS_WORKERS")"
POST_CLASSIFY_WORKERS="$(apply_stage_floor "$POST_CLASSIFY_WORKERS")"
SEASON_MATERIALIZE_WORKERS="$(apply_stage_floor "$SEASON_MATERIALIZE_WORKERS")"
ANALYTICS_REFRESH_WORKERS="$(apply_stage_floor "$ANALYTICS_REFRESH_WORKERS")"
MEDIA_MIRROR_WORKERS="$(apply_stage_floor "$MEDIA_MIRROR_WORKERS")"
COMMENT_MEDIA_MIRROR_WORKERS="$(apply_stage_floor "$COMMENT_MEDIA_MIRROR_WORKERS")"

if [[ "$POSTS_WORKERS" -eq 0 && "$COMMENTS_WORKERS" -eq 0 && "$SHARED_ACCOUNT_POSTS_WORKERS" -eq 0 && "$POST_CLASSIFY_WORKERS" -eq 0 && "$SEASON_MATERIALIZE_WORKERS" -eq 0 && "$ANALYTICS_REFRESH_WORKERS" -eq 0 && "$MEDIA_MIRROR_WORKERS" -eq 0 && "$COMMENT_MEDIA_MIRROR_WORKERS" -eq 0 ]]; then
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

if [[ "$SHARED_ACCOUNT_POSTS_WORKERS" -gt 0 ]]; then
  start_worker "shared_account_posts" --stage shared_account_posts --parallel "$SHARED_ACCOUNT_POSTS_WORKERS" --interval "$WORKER_INTERVAL"
fi

if [[ "$POST_CLASSIFY_WORKERS" -gt 0 ]]; then
  start_worker "post_classify" --stage post_classify --parallel "$POST_CLASSIFY_WORKERS" --interval "$WORKER_INTERVAL"
fi

if [[ "$SEASON_MATERIALIZE_WORKERS" -gt 0 ]]; then
  start_worker "season_materialize" --stage season_materialize --parallel "$SEASON_MATERIALIZE_WORKERS" --interval "$WORKER_INTERVAL"
fi

if [[ "$ANALYTICS_REFRESH_WORKERS" -gt 0 ]]; then
  start_worker "analytics_refresh" --stage analytics_refresh --parallel "$ANALYTICS_REFRESH_WORKERS" --interval "$WORKER_INTERVAL"
fi

if [[ "$MEDIA_MIRROR_WORKERS" -gt 0 ]]; then
  start_worker "media_mirror" --stage media_mirror --parallel "$MEDIA_MIRROR_WORKERS" --interval "$WORKER_INTERVAL"
fi

if [[ "$COMMENT_MEDIA_MIRROR_WORKERS" -gt 0 ]]; then
  start_worker "comment_media_mirror" --stage comment_media_mirror --parallel "$COMMENT_MEDIA_MIRROR_WORKERS" --interval "$WORKER_INTERVAL"
fi

echo "[social-worker-pool] started (posts=${POSTS_WORKERS}, comments=${COMMENTS_WORKERS}, shared_account_posts=${SHARED_ACCOUNT_POSTS_WORKERS}, post_classify=${POST_CLASSIFY_WORKERS}, season_materialize=${SEASON_MATERIALIZE_WORKERS}, analytics_refresh=${ANALYTICS_REFRESH_WORKERS}, media_mirror=${MEDIA_MIRROR_WORKERS}, comment_media_mirror=${COMMENT_MEDIA_MIRROR_WORKERS}, min_stage_runners=${MIN_STAGE_RUNNERS}, allow_stage_disable=${ALLOW_STAGE_DISABLE})"
wait
