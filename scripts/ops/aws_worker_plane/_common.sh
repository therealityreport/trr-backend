#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
DEFAULT_ENV="staging"
DEFAULT_REGION="us-east-1"
DEFAULT_PROFILE="socializer-admin"

ENV_NAME="${WORKSPACE_ENV:-$DEFAULT_ENV}"
REGION="${AWS_REGION:-$DEFAULT_REGION}"
PROFILE="${AWS_PROFILE:-$DEFAULT_PROFILE}"
APPLY=0
DRY_RUN=0
TIMEOUT_SECONDS=900
EVIDENCE_DIR=""

usage_common() {
  cat <<USAGE
Common flags:
  --env <name>            Environment name (default: ${DEFAULT_ENV})
  --region <aws-region>   AWS region (default: ${DEFAULT_REGION})
  --profile <aws-profile> AWS profile (default: ${DEFAULT_PROFILE})
  --evidence-dir <path>   Evidence directory (default: timestamp under docs/ai/evidence/aws-worker-plane)
  --apply                 Execute mutating actions
  --dry-run               Print actions only, no mutation
  --timeout-seconds <n>   Scenario timeout in seconds (default: 900)
  -h, --help              Show help
USAGE
}

parse_common_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --env)
        ENV_NAME="$2"
        shift 2
        ;;
      --region)
        REGION="$2"
        shift 2
        ;;
      --profile)
        PROFILE="$2"
        shift 2
        ;;
      --evidence-dir)
        EVIDENCE_DIR="$2"
        shift 2
        ;;
      --apply)
        APPLY=1
        shift
        ;;
      --dry-run)
        DRY_RUN=1
        shift
        ;;
      --timeout-seconds)
        TIMEOUT_SECONDS="$2"
        shift 2
        ;;
      -h|--help)
        usage_common
        exit 0
        ;;
      *)
        echo "Unknown flag: $1" >&2
        usage_common >&2
        exit 2
        ;;
    esac
  done

  export AWS_PROFILE="$PROFILE"
  export AWS_REGION="$REGION"

  if [[ -z "$EVIDENCE_DIR" ]]; then
    local ts
    ts="$(date +%Y%m%d-%H%M%S)"
    EVIDENCE_DIR="$REPO_ROOT/docs/ai/evidence/aws-worker-plane/$ts"
  fi

  mkdir -p "$EVIDENCE_DIR" "$EVIDENCE_DIR/ssm_outputs" "$EVIDENCE_DIR/db_snapshots"

  export ENV_NAME REGION PROFILE APPLY DRY_RUN TIMEOUT_SECONDS EVIDENCE_DIR SCRIPT_DIR REPO_ROOT
}

require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Required command not found: $cmd" >&2
    exit 1
  fi
}

require_deps() {
  require_cmd aws
  require_cmd jq
  require_cmd curl
  require_cmd psql
}

log() {
  printf '[%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"
}

run_or_echo() {
  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "DRY-RUN: $*"
    return 0
  fi
  "$@"
}

context_path() {
  echo "$EVIDENCE_DIR/context.json"
}

load_context() {
  local ctx
  ctx="$(context_path)"
  if [[ ! -f "$ctx" ]]; then
    echo "Missing context file: $ctx (run 00_discover_context.sh first)" >&2
    exit 1
  fi
  export CONTEXT_JSON="$ctx"
}

get_ctx() {
  local jq_expr="$1"
  jq -r "$jq_expr" "$CONTEXT_JSON"
}

aws_json() {
  local output_file="$1"
  shift
  aws "$@" --output json > "$output_file"
}

ssm_send_and_wait() {
  local instance_id="$1"
  local comment="$2"
  local commands_json="$3"
  local out_file="$4"

  local cmd_id
  cmd_id="$(aws ssm send-command \
    --instance-ids "$instance_id" \
    --document-name 'AWS-RunShellScript' \
    --comment "$comment" \
    --timeout-seconds "$TIMEOUT_SECONDS" \
    --parameters "$commands_json" \
    --query 'Command.CommandId' \
    --output text)"

  local started
  started="$(date +%s)"
  local status=""
  while true; do
    status="$(aws ssm get-command-invocation \
      --command-id "$cmd_id" \
      --instance-id "$instance_id" \
      --query 'Status' \
      --output text 2>/dev/null || true)"
    if [[ "$status" =~ ^(Success|Failed|Cancelled|TimedOut|Cancelling)$ ]]; then
      break
    fi
    if (( $(date +%s) - started > TIMEOUT_SECONDS + 180 )); then
      echo "Timed out waiting for SSM command completion: command_id=$cmd_id status=${status:-unknown}" >&2
      break
    fi
    sleep 5
  done

  aws ssm get-command-invocation \
    --command-id "$cmd_id" \
    --instance-id "$instance_id" \
    --output json > "$out_file"

  echo "$cmd_id"
}

ensure_admin_token() {
  if [[ -n "${TRR_TEST_ADMIN_BEARER_TOKEN:-}" ]]; then
    return 0
  fi
  TRR_TEST_ADMIN_BEARER_TOKEN="$(aws ssm get-parameter \
    --name "/trr/${ENV_NAME}/SUPABASE_SERVICE_ROLE_KEY" \
    --with-decryption \
    --query 'Parameter.Value' \
    --output text)"
  export TRR_TEST_ADMIN_BEARER_TOKEN
}

ensure_api_base_url() {
  if [[ -z "${TRR_API_BASE_URL:-}" ]]; then
    TRR_API_BASE_URL="https://api.thereality.report"
  fi
  export TRR_API_BASE_URL
}

resolve_latest_show_id() {
  local db_url
  db_url="$(aws ssm get-parameter --name "/trr/${ENV_NAME}/DATABASE_URL" --with-decryption --query 'Parameter.Value' --output text)"
  psql "$db_url" -t -A -c "select id::text from core.shows order by updated_at desc nulls last, created_at desc nulls last limit 1;" | head -n1
}

resolve_latest_person_id() {
  local db_url
  db_url="$(aws ssm get-parameter --name "/trr/${ENV_NAME}/DATABASE_URL" --with-decryption --query 'Parameter.Value' --output text)"
  psql "$db_url" -t -A -c "select id::text from core.people order by updated_at desc nulls last, created_at desc nulls last limit 1;" | head -n1
}

append_unique_line() {
  local file="$1"
  local line="$2"
  touch "$file"
  if ! rg -Fxq "$line" "$file"; then
    echo "$line" >> "$file"
  fi
}

health_wait() {
  local url="$1"
  local timeout="$2"
  local started
  started="$(date +%s)"
  while true; do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    local now
    now="$(date +%s)"
    if (( now - started > timeout )); then
      return 1
    fi
    sleep 5
  done
}
