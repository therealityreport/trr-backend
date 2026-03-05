#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=_common.sh
source "$SCRIPT_DIR/_common.sh"
parse_common_args "$@"
require_deps
load_context

WORKER_INSTANCE_ID="$(get_ctx '.asg.worker.instance_id')"
if [[ -z "$WORKER_INSTANCE_ID" || "$WORKER_INSTANCE_ID" == "null" ]]; then
  echo "Missing worker instance id in context.json" >&2
  exit 1
fi

OUT_FILE="$EVIDENCE_DIR/ssm_outputs/worker_units_$(date +%Y%m%d-%H%M%S).json"
CMD_JSON='{"commands":["set -e","for u in trr-admin-operations-worker.service trr-reddit-refresh-worker.service trr-google-news-worker.service trr-social-worker-pool.service; do a=$(systemctl is-active \"$u\" || true); e=$(systemctl is-enabled \"$u\" || true); printf \"%s active=%s enabled=%s\\n\" \"$u\" \"$a\" \"$e\"; done"]}'

log "Validating worker units via SSM: $WORKER_INSTANCE_ID"
ssm_send_and_wait "$WORKER_INSTANCE_ID" "Task11 validate worker units" "$CMD_JSON" "$OUT_FILE" >/dev/null

RAW_STDOUT="$EVIDENCE_DIR/ssm_outputs/worker_units_stdout.txt"
jq -r '.StandardOutputContent // ""' "$OUT_FILE" > "$RAW_STDOUT"
cat "$RAW_STDOUT"

for unit in trr-admin-operations-worker.service trr-reddit-refresh-worker.service trr-google-news-worker.service trr-social-worker-pool.service; do
  if ! rg -q "^${unit} active=active enabled=enabled$" "$RAW_STDOUT"; then
    echo "Unit validation failed: $unit is not active+enabled" >&2
    exit 1
  fi
done

log "Worker unit validation passed"
