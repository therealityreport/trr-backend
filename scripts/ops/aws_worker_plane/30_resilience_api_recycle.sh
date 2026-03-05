#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=_common.sh
source "$SCRIPT_DIR/_common.sh"
parse_common_args "$@"
require_deps
load_context
ensure_admin_token
ensure_api_base_url

API_INSTANCE_ID="$(aws autoscaling describe-auto-scaling-groups --auto-scaling-group-names trr-api-asg --query 'AutoScalingGroups[0].Instances[?LifecycleState==`InService`].InstanceId | [0]' --output text)"
SHOW_ID="${TRR_TEST_SHOW_ID:-$(resolve_latest_show_id)}"
REQUEST_ID="task11-api-recycle-$(date +%s)"
SCENARIO_LOG="$EVIDENCE_DIR/scenario_api_recycle.log"
TRANSCRIPT="$EVIDENCE_DIR/scenario_api_recycle_stream.txt"
STATUS_JSON="$EVIDENCE_DIR/scenario_api_recycle_operation_status.json"
PAYLOAD="$(jq -nc --arg show "$SHOW_ID" '{
  entity_type: "show",
  show_id: $show,
  source_url: "https://task11.example/source",
  images: [
    range(1; 21) | {
      candidate_id: ("task11-api-recycle-" + (. | tostring)),
      url: ("https://picsum.photos/seed/task11-api-recycle-" + (. | tostring) + "/640/360"),
      kind: "other"
    }
  ]
}')"

touch "$SCENARIO_LOG" "$TRANSCRIPT"

log "Starting API recycle resilience scenario for show_id=$SHOW_ID" | tee -a "$SCENARIO_LOG"

curl --max-time "$TIMEOUT_SECONDS" -sS -N -X POST \
  "$TRR_API_BASE_URL/api/v1/admin/scrape/import/stream" \
  -H "Authorization: Bearer $TRR_TEST_ADMIN_BEARER_TOKEN" \
  -H "Content-Type: application/json" \
  -H "x-trr-request-id: $REQUEST_ID" \
  --data "$PAYLOAD" > "$TRANSCRIPT" 2>>"$SCENARIO_LOG" &
STREAM_PID=$!

operation_id=""
for _ in {1..24}; do
  sleep 5
  operation_id="$(rg -o '"operation_id"\s*:\s*"[0-9a-f-]{36}"' "$TRANSCRIPT" | head -n1 | sed -E 's/.*"([0-9a-f-]{36})"/\1/' || true)"
  if [[ -n "$operation_id" ]]; then
    break
  fi
done

if [[ -z "$operation_id" ]]; then
  log "No operation_id via public ALB stream; attempting SSM-local kickoff fallback" | tee -a "$SCENARIO_LOG"
  kill "$STREAM_PID" >/dev/null 2>&1 || true
  wait "$STREAM_PID" >/dev/null 2>&1 || true
  SSM_OUT="$EVIDENCE_DIR/ssm_outputs/api_recycle_kickoff_fallback.json"
  FALLBACK_CMD=$(jq -nc --arg req "$REQUEST_ID-fallback" --arg payload "$PAYLOAD" '{commands:[
    "set -e",
    "TOKEN=$(grep \"^SUPABASE_SERVICE_ROLE_KEY=\" /etc/trr-api.env | cut -d= -f2-)",
    "curl --max-time 180 -sS -N -X POST http://127.0.0.1:8000/api/v1/admin/scrape/import/stream -H \"Authorization: Bearer ${TOKEN}\" -H \"Content-Type: application/json\" -H \"x-trr-request-id: \($req)\" --data '\''\($payload)'\'' | sed -n \"1,80p\""
  ]}')
  ssm_send_and_wait "$API_INSTANCE_ID" "Task11 API recycle fallback kickoff" "$FALLBACK_CMD" "$SSM_OUT" >/dev/null
  jq -r '.StandardOutputContent // ""' "$SSM_OUT" >> "$TRANSCRIPT"
  operation_id="$(rg -o '"operation_id"\s*:\s*"[0-9a-f-]{36}"' "$TRANSCRIPT" | head -n1 | sed -E 's/.*"([0-9a-f-]{36})"/\1/' || true)"
  if [[ -z "$operation_id" ]]; then
    echo "Failed to capture operation_id from stream transcript and SSM fallback" | tee -a "$SCENARIO_LOG" >&2
    exit 1
  fi
fi

append_unique_line "$EVIDENCE_DIR/operation_ids.txt" "$operation_id"
append_unique_line "$EVIDENCE_DIR/request_ids.txt" "$REQUEST_ID"

log "Captured operation_id=$operation_id; rebooting API instance $API_INSTANCE_ID" | tee -a "$SCENARIO_LOG"
run_or_echo aws ec2 reboot-instances --instance-ids "$API_INSTANCE_ID"

if ! health_wait "$TRR_API_BASE_URL/health" "$TIMEOUT_SECONDS"; then
  kill "$STREAM_PID" >/dev/null 2>&1 || true
  wait "$STREAM_PID" >/dev/null 2>&1 || true
  echo "API health did not recover within timeout" | tee -a "$SCENARIO_LOG" >&2
  exit 1
fi

log "API health recovered; polling operation status" | tee -a "$SCENARIO_LOG"
terminal=""
for _ in {1..60}; do
  if curl -fsS -H "Authorization: Bearer $TRR_TEST_ADMIN_BEARER_TOKEN" \
    "$TRR_API_BASE_URL/api/v1/admin/operations/$operation_id" > "$STATUS_JSON"; then
    status="$(jq -r '.operation.status // empty' "$STATUS_JSON")"
    echo "status=$status" | tee -a "$SCENARIO_LOG"
    if [[ "$status" =~ ^(completed|failed|cancelled)$ ]]; then
      terminal="$status"
      break
    fi
  fi
  sleep 10
done

kill "$STREAM_PID" >/dev/null 2>&1 || true
wait "$STREAM_PID" >/dev/null 2>&1 || true

if [[ -z "$terminal" ]]; then
  echo "Operation did not reach terminal state within timeout" | tee -a "$SCENARIO_LOG" >&2
  exit 1
fi

log "API recycle scenario passed with terminal status=$terminal" | tee -a "$SCENARIO_LOG"
