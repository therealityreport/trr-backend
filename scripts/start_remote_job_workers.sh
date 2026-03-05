#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ -f .venv/bin/activate ]]; then
  # shellcheck disable=SC1091
  source .venv/bin/activate
fi

ADMIN_ENABLED="${TRR_ADMIN_OPERATION_WORKER_ENABLED:-1}"
REDDIT_ENABLED="${TRR_REDDIT_REFRESH_WORKER_ENABLED:-1}"
GOOGLE_NEWS_ENABLED="${TRR_GOOGLE_NEWS_WORKER_ENABLED:-1}"
SOCIAL_INGEST_ENABLED="${TRR_SOCIAL_INGEST_WORKER_ENABLED:-0}"
ADMIN_COUNT="${TRR_ADMIN_OPERATION_WORKER_COUNT:-1}"
REDDIT_COUNT="${TRR_REDDIT_REFRESH_WORKER_COUNT:-1}"
GOOGLE_NEWS_COUNT="${TRR_GOOGLE_NEWS_WORKER_COUNT:-1}"
POLL_SECONDS="${TRR_REMOTE_WORKER_POLL_SECONDS:-2}"
GOOGLE_NEWS_LEASE_SECONDS="${TRR_GOOGLE_NEWS_WORKER_LEASE_SECONDS:-300}"
SOCIAL_POLL_SECONDS="${TRR_SOCIAL_INGEST_WORKER_POLL_SECONDS:-3}"
SOCIAL_POSTS_WORKERS="${TRR_SOCIAL_INGEST_WORKER_POSTS:-2}"
SOCIAL_COMMENTS_WORKERS="${TRR_SOCIAL_INGEST_WORKER_COMMENTS:-2}"
SOCIAL_MEDIA_MIRROR_WORKERS="${TRR_SOCIAL_INGEST_WORKER_MEDIA_MIRROR:-1}"
SOCIAL_COMMENT_MEDIA_MIRROR_WORKERS="${TRR_SOCIAL_INGEST_WORKER_COMMENT_MEDIA_MIRROR:-1}"

PIDS=()

start_worker() {
  local label="$1"
  shift
  echo "[remote-workers] starting ${label}: $*"
  "$@" &
  PIDS+=("$!")
}

normalize_count() {
  local raw="$1"
  local label="$2"
  if [[ ! "$raw" =~ ^[0-9]+$ ]]; then
    echo "[remote-workers] WARNING: invalid ${label}='${raw}', using 1" >&2
    echo "1"
    return
  fi
  if [[ "$raw" -lt 1 ]]; then
    echo "[remote-workers] WARNING: ${label} must be >=1 when enabled, using 1" >&2
    echo "1"
    return
  fi
  echo "$raw"
}

normalize_optional_count() {
  local raw="$1"
  local label="$2"
  if [[ ! "$raw" =~ ^[0-9]+$ ]]; then
    echo "[remote-workers] WARNING: invalid ${label}='${raw}', using 0" >&2
    echo "0"
    return
  fi
  echo "$raw"
}

start_worker_group() {
  local label="$1"
  local count="$2"
  shift 2
  for idx in $(seq 1 "$count"); do
    start_worker "${label}#${idx}" "$@" --worker-id "${label}:${idx}"
  done
}

stop_all() {
  if [[ "${#PIDS[@]}" -eq 0 ]]; then
    return
  fi
  echo "[remote-workers] stopping workers..."
  for pid in "${PIDS[@]}"; do
    kill -TERM "$pid" >/dev/null 2>&1 || true
  done
  wait || true
}

trap stop_all EXIT INT TERM

if [[ "$ADMIN_ENABLED" == "1" ]]; then
  ADMIN_COUNT="$(normalize_count "$ADMIN_COUNT" "TRR_ADMIN_OPERATION_WORKER_COUNT")"
  start_worker_group "admin-operations" "$ADMIN_COUNT" python -m scripts.workers.admin_operations_worker --poll-seconds "$POLL_SECONDS"
fi
if [[ "$REDDIT_ENABLED" == "1" ]]; then
  REDDIT_COUNT="$(normalize_count "$REDDIT_COUNT" "TRR_REDDIT_REFRESH_WORKER_COUNT")"
  start_worker_group "reddit-refresh" "$REDDIT_COUNT" python -m scripts.workers.reddit_refresh_worker --poll-seconds "$POLL_SECONDS"
fi
if [[ "$GOOGLE_NEWS_ENABLED" == "1" ]]; then
  GOOGLE_NEWS_COUNT="$(normalize_count "$GOOGLE_NEWS_COUNT" "TRR_GOOGLE_NEWS_WORKER_COUNT")"
  start_worker_group "google-news" "$GOOGLE_NEWS_COUNT" python -m scripts.workers.google_news_worker --poll-seconds "$POLL_SECONDS" --lease-seconds "$GOOGLE_NEWS_LEASE_SECONDS"
fi
if [[ "$SOCIAL_INGEST_ENABLED" == "1" ]]; then
  SOCIAL_POSTS_WORKERS="$(normalize_optional_count "$SOCIAL_POSTS_WORKERS" "TRR_SOCIAL_INGEST_WORKER_POSTS")"
  SOCIAL_COMMENTS_WORKERS="$(normalize_optional_count "$SOCIAL_COMMENTS_WORKERS" "TRR_SOCIAL_INGEST_WORKER_COMMENTS")"
  SOCIAL_MEDIA_MIRROR_WORKERS="$(normalize_optional_count "$SOCIAL_MEDIA_MIRROR_WORKERS" "TRR_SOCIAL_INGEST_WORKER_MEDIA_MIRROR")"
  SOCIAL_COMMENT_MEDIA_MIRROR_WORKERS="$(normalize_optional_count "$SOCIAL_COMMENT_MEDIA_MIRROR_WORKERS" "TRR_SOCIAL_INGEST_WORKER_COMMENT_MEDIA_MIRROR")"
  if [[ "$SOCIAL_POSTS_WORKERS" -gt 0 ]]; then
    start_worker "social-ingest:posts" python -m scripts.socials.worker --stage posts --parallel "$SOCIAL_POSTS_WORKERS" --interval "$SOCIAL_POLL_SECONDS"
  fi
  if [[ "$SOCIAL_COMMENTS_WORKERS" -gt 0 ]]; then
    start_worker "social-ingest:comments" python -m scripts.socials.worker --stage comments --parallel "$SOCIAL_COMMENTS_WORKERS" --interval "$SOCIAL_POLL_SECONDS"
  fi
  if [[ "$SOCIAL_MEDIA_MIRROR_WORKERS" -gt 0 ]]; then
    start_worker "social-ingest:media-mirror" python -m scripts.socials.worker --stage media_mirror --parallel "$SOCIAL_MEDIA_MIRROR_WORKERS" --interval "$SOCIAL_POLL_SECONDS"
  fi
  if [[ "$SOCIAL_COMMENT_MEDIA_MIRROR_WORKERS" -gt 0 ]]; then
    start_worker "social-ingest:comment-media-mirror" python -m scripts.socials.worker --stage comment_media_mirror --parallel "$SOCIAL_COMMENT_MEDIA_MIRROR_WORKERS" --interval "$SOCIAL_POLL_SECONDS"
  fi
fi

if [[ "${#PIDS[@]}" -eq 0 ]]; then
  echo "[remote-workers] no workers enabled; exiting"
  exit 1
fi

echo "[remote-workers] workers started"
wait
