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
REQUEST_ID="task11-screenalytics-outage-$(date +%s)"
SCENARIO_LOG="$EVIDENCE_DIR/scenario_screenalytics_outage.log"
BEFORE_OUT="$EVIDENCE_DIR/scenario_screenalytics_before.txt"
DURING_OUT="$EVIDENCE_DIR/scenario_screenalytics_during.txt"
BACKUP_PATH="/tmp/trr-api.env.task11.$(date +%s).bak"
RESTORE_NEEDED=0

touch "$SCENARIO_LOG"

restore_api_env() {
  if [[ "$RESTORE_NEEDED" -eq 0 ]]; then
    return 0
  fi
  log "Restoring API env from backup: $BACKUP_PATH" | tee -a "$SCENARIO_LOG"
  local restore_json
  restore_json=$(jq -nc --arg backup "$BACKUP_PATH" '{commands:[
    "set -e",
    ("if [ -f " + $backup + " ]; then cp " + $backup + " /etc/trr-api.env; fi"),
    "systemctl restart trr-api",
    "systemctl is-active trr-api"
  ]}')
  ssm_send_and_wait "$API_INSTANCE_ID" "Task11 restore screenalytics outage test" "$restore_json" "$EVIDENCE_DIR/ssm_outputs/screenalytics_restore.json" >/dev/null || true
  RESTORE_NEEDED=0
}
trap restore_api_env EXIT

log "Running baseline dependent request before outage" | tee -a "$SCENARIO_LOG"
curl --max-time 90 -sS -N -X POST \
  "$TRR_API_BASE_URL/api/v1/admin/shows/${SHOW_ID}/assets/batch-jobs/stream" \
  -H "Authorization: Bearer $TRR_TEST_ADMIN_BEARER_TOKEN" \
  -H "Content-Type: application/json" \
  -H "x-trr-request-id: ${REQUEST_ID}-before" \
  --data '{"operations":["count"]}' > "$BEFORE_OUT" || true

log "Applying outage override SCREENALYTICS_API_URL=http://127.0.0.1:9" | tee -a "$SCENARIO_LOG"
OUTAGE_JSON=$(jq -nc --arg backup "$BACKUP_PATH" '{commands:[
  "set -e",
  ("cp /etc/trr-api.env " + $backup),
  "if grep -q \"^SCREENALYTICS_API_URL=\" /etc/trr-api.env; then sed -i \"s#^SCREENALYTICS_API_URL=.*#SCREENALYTICS_API_URL=http://127.0.0.1:9#\" /etc/trr-api.env; else echo \"SCREENALYTICS_API_URL=http://127.0.0.1:9\" >> /etc/trr-api.env; fi",
  "systemctl restart trr-api",
  "systemctl is-active trr-api"
]}')
ssm_send_and_wait "$API_INSTANCE_ID" "Task11 simulate screenalytics outage" "$OUTAGE_JSON" "$EVIDENCE_DIR/ssm_outputs/screenalytics_outage_apply.json" >/dev/null
RESTORE_NEEDED=1

if ! health_wait "$TRR_API_BASE_URL/health" "$TIMEOUT_SECONDS"; then
  echo "API did not recover after outage override" | tee -a "$SCENARIO_LOG" >&2
  exit 1
fi

log "Running dependent request during outage" | tee -a "$SCENARIO_LOG"
curl --max-time 90 -sS -N -X POST \
  "$TRR_API_BASE_URL/api/v1/admin/shows/${SHOW_ID}/assets/batch-jobs/stream" \
  -H "Authorization: Bearer $TRR_TEST_ADMIN_BEARER_TOKEN" \
  -H "Content-Type: application/json" \
  -H "x-trr-request-id: ${REQUEST_ID}-during" \
  --data '{"operations":["count"]}' > "$DURING_OUT" || true

if cmp -s "$BEFORE_OUT" "$DURING_OUT"; then
  log "Warning: outage response matched baseline exactly; review manually" | tee -a "$SCENARIO_LOG"
else
  log "Outage produced behavior delta (expected for dependency disruption)" | tee -a "$SCENARIO_LOG"
fi

restore_api_env
if ! health_wait "$TRR_API_BASE_URL/health" "$TIMEOUT_SECONDS"; then
  echo "API did not recover after outage rollback" | tee -a "$SCENARIO_LOG" >&2
  exit 1
fi

append_unique_line "$EVIDENCE_DIR/request_ids.txt" "${REQUEST_ID}-before"
append_unique_line "$EVIDENCE_DIR/request_ids.txt" "${REQUEST_ID}-during"
log "Screenalytics outage scenario completed and rollback verified" | tee -a "$SCENARIO_LOG"
