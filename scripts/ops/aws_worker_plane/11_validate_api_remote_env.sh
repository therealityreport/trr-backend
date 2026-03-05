#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=_common.sh
source "$SCRIPT_DIR/_common.sh"
parse_common_args "$@"
require_deps
load_context

API_INSTANCE_ID="$(get_ctx '.asg.api.instance_id')"
if [[ -z "$API_INSTANCE_ID" || "$API_INSTANCE_ID" == "null" ]]; then
  echo "Missing api instance id in context.json" >&2
  exit 1
fi

OUT_FILE="$EVIDENCE_DIR/ssm_outputs/api_env_$(date +%Y%m%d-%H%M%S).json"
CMD_JSON='{"commands":["set -e","echo [systemctl_show]","systemctl show trr-api -p EnvironmentFiles || true","echo [grep_flags]","grep -E \"^TRR_JOB_PLANE_MODE=|^TRR_LONG_JOB_ENFORCE_REMOTE=\" /etc/trr-api.env || true"]}'

log "Validating API remote env via SSM: $API_INSTANCE_ID"
ssm_send_and_wait "$API_INSTANCE_ID" "Task11 validate api remote env" "$CMD_JSON" "$OUT_FILE" >/dev/null

RAW_STDOUT="$EVIDENCE_DIR/ssm_outputs/api_env_remote_mode.txt"
jq -r '.StandardOutputContent // ""' "$OUT_FILE" > "$RAW_STDOUT"
cat "$RAW_STDOUT"

if ! rg -q 'TRR_JOB_PLANE_MODE=remote' "$RAW_STDOUT"; then
  echo "Missing TRR_JOB_PLANE_MODE=remote in API runtime" >&2
  exit 1
fi
if ! rg -q 'TRR_LONG_JOB_ENFORCE_REMOTE=1' "$RAW_STDOUT"; then
  echo "Missing TRR_LONG_JOB_ENFORCE_REMOTE=1 in API runtime" >&2
  exit 1
fi

log "API remote env validation passed"
