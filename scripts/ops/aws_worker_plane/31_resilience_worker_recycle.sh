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

WORKER_INSTANCE_ID="$(aws autoscaling describe-auto-scaling-groups --auto-scaling-group-names trr-worker-asg --query 'AutoScalingGroups[0].Instances[?LifecycleState==`InService`].InstanceId | [0]' --output text)"
SHOW_ID="${TRR_TEST_SHOW_ID:-$(resolve_latest_show_id)}"
REQUEST_ID="task11-worker-recycle-$(date +%s)"
SCENARIO_LOG="$EVIDENCE_DIR/scenario_worker_recycle.log"
TRANSCRIPT="$EVIDENCE_DIR/scenario_worker_recycle_stream.txt"
STATUS_JSON="$EVIDENCE_DIR/scenario_worker_recycle_operation_status.json"
PAYLOAD="$(jq -nc --arg show "$SHOW_ID" '{
  entity_type: "show",
  show_id: $show,
  source_url: "https://task11.example/source",
  images: [
    range(1; 21) | {
      candidate_id: ("task11-worker-recycle-" + (. | tostring)),
      url: ("https://picsum.photos/seed/task11-worker-recycle-" + (. | tostring) + "/640/360"),
      kind: "other"
    }
  ]
}')"

touch "$SCENARIO_LOG" "$TRANSCRIPT"

log "Starting worker recycle resilience scenario for show_id=$SHOW_ID" | tee -a "$SCENARIO_LOG"

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
  API_INSTANCE_ID="$(aws autoscaling describe-auto-scaling-groups --auto-scaling-group-names trr-api-asg --query 'AutoScalingGroups[0].Instances[?LifecycleState==`InService`].InstanceId | [0]' --output text)"
  SSM_OUT="$EVIDENCE_DIR/ssm_outputs/worker_recycle_kickoff_fallback.json"
  FALLBACK_CMD=$(jq -nc --arg req "$REQUEST_ID-fallback" --arg payload "$PAYLOAD" '{commands:[
    "set -e",
    "TOKEN=$(grep \"^SUPABASE_SERVICE_ROLE_KEY=\" /etc/trr-api.env | cut -d= -f2-)",
    "curl --max-time 180 -sS -N -X POST http://127.0.0.1:8000/api/v1/admin/scrape/import/stream -H \"Authorization: Bearer ${TOKEN}\" -H \"Content-Type: application/json\" -H \"x-trr-request-id: \($req)\" --data '\''\($payload)'\'' | sed -n \"1,80p\""
  ]}')
  ssm_send_and_wait "$API_INSTANCE_ID" "Task11 worker recycle fallback kickoff" "$FALLBACK_CMD" "$SSM_OUT" >/dev/null
  jq -r '.StandardOutputContent // ""' "$SSM_OUT" >> "$TRANSCRIPT"
  operation_id="$(rg -o '"operation_id"\s*:\s*"[0-9a-f-]{36}"' "$TRANSCRIPT" | head -n1 | sed -E 's/.*"([0-9a-f-]{36})"/\1/' || true)"
  if [[ -z "$operation_id" ]]; then
    echo "Failed to capture operation_id from stream transcript and SSM fallback" | tee -a "$SCENARIO_LOG" >&2
    exit 1
  fi
fi

append_unique_line "$EVIDENCE_DIR/operation_ids.txt" "$operation_id"
append_unique_line "$EVIDENCE_DIR/request_ids.txt" "$REQUEST_ID"

log "Captured operation_id=$operation_id; rebooting worker instance $WORKER_INSTANCE_ID" | tee -a "$SCENARIO_LOG"
run_or_echo aws ec2 reboot-instances --instance-ids "$WORKER_INSTANCE_ID"

log "Waiting for worker instance status checks" | tee -a "$SCENARIO_LOG"
run_or_echo aws ec2 wait instance-status-ok --instance-ids "$WORKER_INSTANCE_ID"

PING_DEADLINE=$(( $(date +%s) + TIMEOUT_SECONDS ))
while true; do
  ping="$(aws ssm describe-instance-information --filters "Key=InstanceIds,Values=$WORKER_INSTANCE_ID" --query 'InstanceInformationList[0].PingStatus' --output text || true)"
  echo "ssm_ping=$ping" | tee -a "$SCENARIO_LOG"
  if [[ "$ping" == "Online" ]]; then
    break
  fi
  if (( $(date +%s) > PING_DEADLINE )); then
    echo "Worker did not return Online in SSM" | tee -a "$SCENARIO_LOG" >&2
    kill "$STREAM_PID" >/dev/null 2>&1 || true
    wait "$STREAM_PID" >/dev/null 2>&1 || true
    exit 1
  fi
  sleep 10
done

UNITS_JSON="$EVIDENCE_DIR/ssm_outputs/worker_recycle_units.json"
CMD_JSON='{"commands":["set -e","for u in trr-admin-operations-worker.service trr-reddit-refresh-worker.service trr-google-news-worker.service trr-social-worker-pool.service; do a=$(systemctl is-active \"$u\" || true); e=$(systemctl is-enabled \"$u\" || true); printf \"%s active=%s enabled=%s\\n\" \"$u\" \"$a\" \"$e\"; done"]}'
ssm_send_and_wait "$WORKER_INSTANCE_ID" "Task11 worker recycle unit check" "$CMD_JSON" "$UNITS_JSON" >/dev/null
jq -r '.StandardOutputContent // ""' "$UNITS_JSON" | tee "$EVIDENCE_DIR/ssm_outputs/worker_recycle_units_stdout.txt"

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
  echo "Operation did not reach terminal state after worker recycle" | tee -a "$SCENARIO_LOG" >&2
  exit 1
fi

log "Worker recycle scenario passed with terminal status=$terminal" | tee -a "$SCENARIO_LOG"
