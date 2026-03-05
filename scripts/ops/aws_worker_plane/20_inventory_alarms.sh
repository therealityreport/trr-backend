#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=_common.sh
source "$SCRIPT_DIR/_common.sh"
parse_common_args "$@"
require_deps
load_context

OUT="$EVIDENCE_DIR/alarm_inventory.json"
aws cloudwatch describe-alarms --output json > "$OUT"

jq -r '.MetricAlarms[].AlarmName' "$OUT" | sort > "$EVIDENCE_DIR/alarm_inventory_names.txt"
cat "$EVIDENCE_DIR/alarm_inventory_names.txt"

LINKS="$EVIDENCE_DIR/cloudwatch_links.md"
{
  echo "# CloudWatch Links"
  echo
  echo "## Alarms"
  while IFS= read -r alarm; do
    [[ -z "$alarm" ]] && continue
    echo "- https://${REGION}.console.aws.amazon.com/cloudwatch/home?region=${REGION}#alarmsV2:alarm/${alarm}"
  done < "$EVIDENCE_DIR/alarm_inventory_names.txt"
  echo
  echo "## Log Groups"
  aws logs describe-log-groups --query 'logGroups[].logGroupName' --output text | tr '\t' '\n' | sort | while IFS= read -r group; do
    [[ -z "$group" ]] && continue
    encoded_group="${group//\//%252F}"
    echo "- https://${REGION}.console.aws.amazon.com/cloudwatch/home?region=${REGION}#logsV2:log-groups/log-group/${encoded_group}"
  done
} > "$LINKS"

log "Alarm inventory captured: $OUT"
