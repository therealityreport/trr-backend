#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=_common.sh
source "$SCRIPT_DIR/_common.sh"
parse_common_args "$@"
require_deps

log "Discovering staging context (env=$ENV_NAME region=$REGION profile=$PROFILE)"

API_ASG_NAME="trr-api-asg"
WORKER_ASG_NAME="trr-worker-asg"
ALB_NAME="trr-api-alb"
TG_NAME="trr-api-tg"

API_INSTANCE_ID="$(aws autoscaling describe-auto-scaling-groups --auto-scaling-group-names "$API_ASG_NAME" --query 'AutoScalingGroups[0].Instances[?LifecycleState==`InService`].InstanceId | [0]' --output text)"
WORKER_INSTANCE_ID="$(aws autoscaling describe-auto-scaling-groups --auto-scaling-group-names "$WORKER_ASG_NAME" --query 'AutoScalingGroups[0].Instances[?LifecycleState==`InService`].InstanceId | [0]' --output text)"

ALB_ARN="$(aws elbv2 describe-load-balancers --names "$ALB_NAME" --query 'LoadBalancers[0].LoadBalancerArn' --output text)"
TG_ARN="$(aws elbv2 describe-target-groups --names "$TG_NAME" --query 'TargetGroups[0].TargetGroupArn' --output text)"

ALL_LOG_GROUPS_JSON="$EVIDENCE_DIR/all_log_groups.json"
aws logs describe-log-groups --query 'logGroups[].logGroupName' --output json > "$ALL_LOG_GROUPS_JSON"

jq -n \
  --arg env "$ENV_NAME" \
  --arg region "$REGION" \
  --arg profile "$PROFILE" \
  --arg api_asg "$API_ASG_NAME" \
  --arg worker_asg "$WORKER_ASG_NAME" \
  --arg api_instance_id "$API_INSTANCE_ID" \
  --arg worker_instance_id "$WORKER_INSTANCE_ID" \
  --arg alb_name "$ALB_NAME" \
  --arg alb_arn "$ALB_ARN" \
  --arg tg_name "$TG_NAME" \
  --arg tg_arn "$TG_ARN" \
  --arg health_path "/health" \
  --arg api_base_url "${TRR_API_BASE_URL:-https://api.thereality.report}" \
  --slurpfile log_groups "$ALL_LOG_GROUPS_JSON" \
  '{
      generated_at: now | todate,
      env: $env,
      region: $region,
      profile: $profile,
      asg: {
        api: { name: $api_asg, instance_id: $api_instance_id },
        worker: { name: $worker_asg, instance_id: $worker_instance_id }
      },
      alb: { name: $alb_name, arn: $alb_arn, health_path: $health_path },
      target_group: { name: $tg_name, arn: $tg_arn },
      service_units: [
        "trr-admin-operations-worker.service",
        "trr-reddit-refresh-worker.service",
        "trr-google-news-worker.service",
        "trr-social-worker-pool.service"
      ],
      expected_alarms: [
        "trr-api-target-5xx",
        "trr-worker-zero-inservice",
        "trr-worker-status-check-failed",
        "trr-queue-depth-high",
        "trr-stale-leases-high",
        "trr-long-job-failures-high",
        "trr-worker-service-failure-signal"
      ],
      expected_cloudwatch_log_groups: [
        "/trr/ec2/cloud-init",
        "/trr/ec2/cloud-init-output"
      ],
      discovered_log_groups: ($log_groups[0] // []),
      api_base_url: $api_base_url
    }' > "$(context_path)"

log "Context written: $(context_path)"
