#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=_common.sh
source "$SCRIPT_DIR/_common.sh"
parse_common_args "$@"
require_deps
load_context

API_INSTANCE_ID="$(get_ctx '.asg.api.instance_id')"
WORKER_INSTANCE_ID="$(get_ctx '.asg.worker.instance_id')"

GROUPS_FILE="$EVIDENCE_DIR/cloudwatch_groups.json"
aws logs describe-log-groups --query 'logGroups[].logGroupName' --output json > "$GROUPS_FILE"

API_GROUPS=$(jq -r '.[] | select(test("/trr/api|/api"; "i"))' "$GROUPS_FILE" | paste -sd ',' -)
WORKER_GROUPS=$(jq -r '.[] | select(test("/trr/worker|/worker"; "i"))' "$GROUPS_FILE" | paste -sd ',' -)

WARNINGS=()
if [[ -z "$API_GROUPS" ]]; then
  WARNINGS+=("No API-specific CloudWatch log group discovered")
fi
if [[ -z "$WORKER_GROUPS" ]]; then
  WARNINGS+=("No worker-specific CloudWatch log group discovered")
fi

for g in /trr/ec2/cloud-init /trr/ec2/cloud-init-output; do
  if ! jq -e --arg g "$g" '.[] | select(. == $g)' "$GROUPS_FILE" >/dev/null; then
    WARNINGS+=("Expected cloud-init log group missing: $g")
  fi
done

check_group_events() {
  local group="$1"
  local marker="$2"
  local start_ms
  start_ms=$(( ( $(date +%s) - 21600 ) * 1000 ))
  aws logs filter-log-events --log-group-name "$group" --start-time "$start_ms" --query 'events[].message' --output text 2>/dev/null | rg -c "$marker" || true
}

API_EVENT_SUM=0
WORKER_EVENT_SUM=0
while IFS= read -r group; do
  [[ -z "$group" ]] && continue
  count=$(check_group_events "$group" "$API_INSTANCE_ID")
  API_EVENT_SUM=$((API_EVENT_SUM + count))
done < <(jq -r '.[] | select(test("/trr/api|/api"; "i"))' "$GROUPS_FILE")

while IFS= read -r group; do
  [[ -z "$group" ]] && continue
  count=$(check_group_events "$group" "$WORKER_INSTANCE_ID")
  WORKER_EVENT_SUM=$((WORKER_EVENT_SUM + count))
done < <(jq -r '.[] | select(test("/trr/worker|/worker"; "i"))' "$GROUPS_FILE")

UNIT_MARKERS_FOUND=0
while IFS= read -r group; do
  [[ -z "$group" ]] && continue
  marker_count=$(check_group_events "$group" 'trr-admin-operations-worker|trr-reddit-refresh-worker|trr-google-news-worker|trr-social-worker-pool|trr-api')
  UNIT_MARKERS_FOUND=$((UNIT_MARKERS_FOUND + marker_count))
done < <(jq -r '.[] | select(test("/trr/"; "i"))' "$GROUPS_FILE")

if [[ "$API_EVENT_SUM" -eq 0 ]]; then
  WARNINGS+=("No recent API instance events found in discovered API log groups")
fi
if [[ "$WORKER_EVENT_SUM" -eq 0 ]]; then
  WARNINGS+=("No recent worker instance events found in discovered worker log groups")
fi
if [[ "$UNIT_MARKERS_FOUND" -eq 0 ]]; then
  WARNINGS+=("No recent worker/api unit markers found in discovered /trr/* log groups")
fi

jq -n \
  --arg api_instance_id "$API_INSTANCE_ID" \
  --arg worker_instance_id "$WORKER_INSTANCE_ID" \
  --argjson all_groups "$(cat "$GROUPS_FILE")" \
  --arg api_groups_csv "$API_GROUPS" \
  --arg worker_groups_csv "$WORKER_GROUPS" \
  --argjson api_event_sum "$API_EVENT_SUM" \
  --argjson worker_event_sum "$WORKER_EVENT_SUM" \
  --argjson unit_markers_found "$UNIT_MARKERS_FOUND" \
  --argjson warnings "$(printf '%s\n' "${WARNINGS[@]:-}" | jq -R . | jq -s .)" \
  '{
    generated_at: now | todate,
    api_instance_id: $api_instance_id,
    worker_instance_id: $worker_instance_id,
    discovered_log_groups: $all_groups,
    api_groups_csv: $api_groups_csv,
    worker_groups_csv: $worker_groups_csv,
    api_event_sum: $api_event_sum,
    worker_event_sum: $worker_event_sum,
    unit_markers_found: $unit_markers_found,
    warnings: $warnings
  }' > "$EVIDENCE_DIR/cloudwatch_log_presence.json"

cat "$EVIDENCE_DIR/cloudwatch_log_presence.json"

if [[ -z "$API_GROUPS" || -z "$WORKER_GROUPS" ]]; then
  echo "Required API/worker log group discovery failed" >&2
  exit 1
fi

log "CloudWatch log validation completed (warnings may exist; see cloudwatch_log_presence.json)"
