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

SHOW_ID="${TRR_TEST_SHOW_ID:-$(resolve_latest_show_id)}"
REQUEST_ID="task11-sse-replay-$(date +%s)"
INITIAL_STREAM="$EVIDENCE_DIR/scenario_sse_initial_stream.txt"
REPLAY_STREAM="$EVIDENCE_DIR/scenario_sse_replay_stream.txt"
SCENARIO_LOG="$EVIDENCE_DIR/scenario_sse_replay.log"
PAYLOAD="$(jq -nc --arg show "$SHOW_ID" '{
  entity_type: "show",
  show_id: $show,
  source_url: "https://task11.example/source",
  images: [
    range(1; 21) | {
      candidate_id: ("task11-sse-replay-" + (. | tostring)),
      url: ("https://picsum.photos/seed/task11-sse-replay-" + (. | tostring) + "/640/360"),
      kind: "other"
    }
  ]
}')"

touch "$INITIAL_STREAM" "$REPLAY_STREAM" "$SCENARIO_LOG"

log "Starting SSE replay scenario with show_id=$SHOW_ID" | tee -a "$SCENARIO_LOG"

curl --max-time "$TIMEOUT_SECONDS" -sS -N -X POST \
  "$TRR_API_BASE_URL/api/v1/admin/scrape/import/stream" \
  -H "Authorization: Bearer $TRR_TEST_ADMIN_BEARER_TOKEN" \
  -H "Content-Type: application/json" \
  -H "x-trr-request-id: $REQUEST_ID" \
  --data "$PAYLOAD" > "$INITIAL_STREAM" 2>>"$SCENARIO_LOG" &
STREAM_PID=$!

operation_id=""
last_seq=""
for _ in {1..30}; do
  sleep 4
  operation_id="$(rg -o '"operation_id"\s*:\s*"[0-9a-f-]{36}"' "$INITIAL_STREAM" | head -n1 | sed -E 's/.*"([0-9a-f-]{36})"/\1/' || true)"
  last_seq="$(rg -o '"event_seq"\s*:\s*[0-9]+' "$INITIAL_STREAM" | sed -E 's/.*:[[:space:]]*([0-9]+)/\1/' | tail -n1 || true)"
  if [[ -n "$operation_id" && -n "$last_seq" ]]; then
    break
  fi
done

kill "$STREAM_PID" >/dev/null 2>&1 || true
wait "$STREAM_PID" >/dev/null 2>&1 || true

if [[ -z "$operation_id" || -z "$last_seq" ]]; then
  log "No operation_id/event_seq via public ALB stream; attempting SSM-local kickoff fallback" | tee -a "$SCENARIO_LOG"
  API_INSTANCE_ID="$(aws autoscaling describe-auto-scaling-groups --auto-scaling-group-names trr-api-asg --query 'AutoScalingGroups[0].Instances[?LifecycleState==`InService`].InstanceId | [0]' --output text)"
  SSM_OUT="$EVIDENCE_DIR/ssm_outputs/sse_replay_kickoff_fallback.json"
  FALLBACK_CMD=$(jq -nc --arg req "$REQUEST_ID-fallback" --arg payload "$PAYLOAD" '{commands:[
    "set -e",
    "TOKEN=$(grep \"^SUPABASE_SERVICE_ROLE_KEY=\" /etc/trr-api.env | cut -d= -f2-)",
    "curl --max-time 180 -sS -N -X POST http://127.0.0.1:8000/api/v1/admin/scrape/import/stream -H \"Authorization: Bearer ${TOKEN}\" -H \"Content-Type: application/json\" -H \"x-trr-request-id: \($req)\" --data '\''\($payload)'\'' | sed -n \"1,120p\""
  ]}')
  ssm_send_and_wait "$API_INSTANCE_ID" "Task11 SSE replay fallback kickoff" "$FALLBACK_CMD" "$SSM_OUT" >/dev/null
  jq -r '.StandardOutputContent // ""' "$SSM_OUT" >> "$INITIAL_STREAM"
  operation_id="$(rg -o '"operation_id"\s*:\s*"[0-9a-f-]{36}"' "$INITIAL_STREAM" | head -n1 | sed -E 's/.*"([0-9a-f-]{36})"/\1/' || true)"
  last_seq="$(rg -o '"event_seq"\s*:\s*[0-9]+' "$INITIAL_STREAM" | sed -E 's/.*:[[:space:]]*([0-9]+)/\1/' | tail -n1 || true)"
  if [[ -z "$operation_id" || -z "$last_seq" ]]; then
    echo "Failed to capture operation_id/event_seq from initial stream and SSM fallback" | tee -a "$SCENARIO_LOG" >&2
    exit 1
  fi
fi

append_unique_line "$EVIDENCE_DIR/operation_ids.txt" "$operation_id"
append_unique_line "$EVIDENCE_DIR/request_ids.txt" "$REQUEST_ID"

log "Reconnecting stream from after_seq=$last_seq operation_id=$operation_id" | tee -a "$SCENARIO_LOG"
curl --max-time 120 -sS -N \
  -H "Authorization: Bearer $TRR_TEST_ADMIN_BEARER_TOKEN" \
  "$TRR_API_BASE_URL/api/v1/admin/operations/$operation_id/stream?after_seq=$last_seq" > "$REPLAY_STREAM" 2>>"$SCENARIO_LOG" || true

replay_first_seq="$(rg -o '"event_seq"\s*:\s*[0-9]+' "$REPLAY_STREAM" | sed -E 's/.*:[[:space:]]*([0-9]+)/\1/' | head -n1 || true)"
if [[ -z "$replay_first_seq" ]]; then
  echo "Replay stream contained no event_seq" | tee -a "$SCENARIO_LOG" >&2
  exit 1
fi

if (( replay_first_seq <= last_seq )); then
  echo "Replay sequence regression: replay_first_seq=$replay_first_seq last_seq=$last_seq" | tee -a "$SCENARIO_LOG" >&2
  exit 1
fi

log "SSE replay scenario passed: last_seq=$last_seq replay_first_seq=$replay_first_seq" | tee -a "$SCENARIO_LOG"
