#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=_common.sh
source "$SCRIPT_DIR/_common.sh"
parse_common_args "$@"
require_deps
load_context

PLAN_SCRIPT="$EVIDENCE_DIR/missing_alarm_commands.sh"
if [[ ! -f "$PLAN_SCRIPT" ]]; then
  echo "Missing missing_alarm_commands.sh. Run 21_plan_missing_alarms.sh first." >&2
  exit 1
fi

APPLY_LOG="$EVIDENCE_DIR/alarm_apply.log"
: > "$APPLY_LOG"

if [[ "$APPLY" -eq 1 ]]; then
  log "Applying missing alarm definitions"
  if [[ "$DRY_RUN" -eq 1 ]]; then
    cat "$PLAN_SCRIPT" | tee -a "$APPLY_LOG"
  else
    bash "$PLAN_SCRIPT" | tee -a "$APPLY_LOG"
  fi
else
  log "--apply not set. No alarm mutations executed"
fi

POST="$EVIDENCE_DIR/alarm_post_apply_inventory.json"
aws cloudwatch describe-alarms --output json > "$POST"

required=(
  trr-api-target-5xx
  trr-worker-zero-inservice
  trr-worker-status-check-failed
  trr-queue-depth-high
  trr-stale-leases-high
  trr-long-job-failures-high
  trr-worker-service-failure-signal
)

missing_after=()
for alarm in "${required[@]}"; do
  if ! jq -e --arg a "$alarm" '.MetricAlarms[]? | select(.AlarmName == $a)' "$POST" >/dev/null; then
    missing_after+=("$alarm")
  fi
done

if [[ ${#missing_after[@]} -gt 0 ]]; then
  printf '%s\n' "${missing_after[@]}" > "$EVIDENCE_DIR/missing_after_apply.txt"
  echo "Missing required alarms after apply:" >&2
  cat "$EVIDENCE_DIR/missing_after_apply.txt" >&2
  exit 1
fi

log "Alarm target set satisfied (7 alarms present)"
