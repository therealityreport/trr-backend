#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=_common.sh
source "$SCRIPT_DIR/_common.sh"
parse_common_args "$@"
require_deps

COMMON_ARGS=(--env "$ENV_NAME" --region "$REGION" --profile "$PROFILE" --evidence-dir "$EVIDENCE_DIR" --timeout-seconds "$TIMEOUT_SECONDS")
if [[ "$DRY_RUN" -eq 1 ]]; then
  COMMON_ARGS+=(--dry-run)
fi

log "Running Task11 Phase 7/8/9 pack with evidence dir: $EVIDENCE_DIR"

bash "$SCRIPT_DIR/00_discover_context.sh" "${COMMON_ARGS[@]}"
bash "$SCRIPT_DIR/10_validate_ssm_worker_units.sh" "${COMMON_ARGS[@]}"
bash "$SCRIPT_DIR/11_validate_api_remote_env.sh" "${COMMON_ARGS[@]}"
bash "$SCRIPT_DIR/12_validate_cloudwatch_logs.sh" "${COMMON_ARGS[@]}"
bash "$SCRIPT_DIR/20_inventory_alarms.sh" "${COMMON_ARGS[@]}"
bash "$SCRIPT_DIR/21_plan_missing_alarms.sh" "${COMMON_ARGS[@]}"
if [[ "$APPLY" -eq 1 ]]; then
  bash "$SCRIPT_DIR/22_apply_alarms.sh" "${COMMON_ARGS[@]}" --apply
else
  bash "$SCRIPT_DIR/22_apply_alarms.sh" "${COMMON_ARGS[@]}"
fi

bash "$SCRIPT_DIR/30_resilience_api_recycle.sh" "${COMMON_ARGS[@]}"
bash "$SCRIPT_DIR/31_resilience_worker_recycle.sh" "${COMMON_ARGS[@]}"
bash "$SCRIPT_DIR/32_resilience_sse_replay.sh" "${COMMON_ARGS[@]}"
bash "$SCRIPT_DIR/33_resilience_screenalytics_outage.sh" "${COMMON_ARGS[@]}"

log "Task11 run_all completed"
