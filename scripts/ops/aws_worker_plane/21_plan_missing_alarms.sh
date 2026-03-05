#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=_common.sh
source "$SCRIPT_DIR/_common.sh"
parse_common_args "$@"
require_deps
load_context

INV="$EVIDENCE_DIR/alarm_inventory.json"
if [[ ! -f "$INV" ]]; then
  echo "Missing alarm inventory. Run 20_inventory_alarms.sh first." >&2
  exit 1
fi

MISSING_SCRIPT="$EVIDENCE_DIR/missing_alarm_commands.sh"
DELETE_SCRIPT="$EVIDENCE_DIR/delete_alarm_commands.sh"
: > "$MISSING_SCRIPT"
: > "$DELETE_SCRIPT"
chmod +x "$MISSING_SCRIPT" "$DELETE_SCRIPT"

echo "#!/usr/bin/env bash" >> "$MISSING_SCRIPT"
echo "set -euo pipefail" >> "$MISSING_SCRIPT"
echo "export AWS_PROFILE='$PROFILE'" >> "$MISSING_SCRIPT"
echo "export AWS_REGION='$REGION'" >> "$MISSING_SCRIPT"

echo "#!/usr/bin/env bash" >> "$DELETE_SCRIPT"
echo "set -euo pipefail" >> "$DELETE_SCRIPT"
echo "export AWS_PROFILE='$PROFILE'" >> "$DELETE_SCRIPT"
echo "export AWS_REGION='$REGION'" >> "$DELETE_SCRIPT"

targets=(
  trr-api-target-5xx
  trr-worker-zero-inservice
  trr-worker-status-check-failed
  trr-queue-depth-high
  trr-stale-leases-high
  trr-long-job-failures-high
  trr-worker-service-failure-signal
)

missing=()
for alarm in "${targets[@]}"; do
  if ! jq -e --arg a "$alarm" '.MetricAlarms[]? | select(.AlarmName == $a)' "$INV" >/dev/null; then
    missing+=("$alarm")
  fi
done

ALB_DIMENSION="$(get_ctx '.alb.arn' | sed -E 's#^arn:aws:elasticloadbalancing:[^:]+:[^:]+:loadbalancer/##')"
WORKER_ASG_NAME="$(get_ctx '.asg.worker.name')"
WORKER_INSTANCE_ID="$(get_ctx '.asg.worker.instance_id')"

if printf '%s\n' "${missing[@]:-}" | rg -qx 'trr-worker-service-failure-signal'; then
  FILTER_NAME="trr-worker-service-failure-filter"
  LOG_GROUP="/trr/worker/bootstrap"

  cat >> "$MISSING_SCRIPT" <<CMDS
aws logs put-metric-filter \\
  --log-group-name '$LOG_GROUP' \\
  --filter-name '$FILTER_NAME' \\
  --filter-pattern '?failed ?ERROR ?restart ?Traceback' \\
  --metric-transformations metricName=worker_service_failure_signal,metricNamespace=trr,metricValue=1

aws cloudwatch put-metric-alarm \\
  --alarm-name 'trr-worker-service-failure-signal' \\
  --alarm-description 'Worker unit/service failure signal from CloudWatch Logs metric filter' \\
  --namespace 'trr' \\
  --metric-name 'worker_service_failure_signal' \\
  --statistic Sum \\
  --period 300 \\
  --evaluation-periods 2 \\
  --threshold 5 \\
  --comparison-operator GreaterThanThreshold \\
  --treat-missing-data notBreaching
CMDS

  cat >> "$DELETE_SCRIPT" <<CMDS
aws cloudwatch delete-alarms --alarm-names 'trr-worker-service-failure-signal'
aws logs delete-metric-filter --log-group-name '$LOG_GROUP' --filter-name '$FILTER_NAME'
CMDS
fi

if [[ ${#missing[@]} -eq 0 ]]; then
  jq -n '{missing: []}' > "$EVIDENCE_DIR/missing_alarms.json"
else
  printf '%s\n' "${missing[@]}" | jq -R . | jq -s '{missing: .}' > "$EVIDENCE_DIR/missing_alarms.json"
fi
cat "$EVIDENCE_DIR/missing_alarms.json"
log "Planned missing alarm commands: $MISSING_SCRIPT"
log "Planned rollback commands: $DELETE_SCRIPT"
