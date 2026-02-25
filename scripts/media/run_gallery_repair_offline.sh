#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="/Users/thomashulihan/Projects/TRR/TRR-Backend"
DEFAULT_PYTHON="${REPO_ROOT}/.venv/bin/python"
DEFAULT_SOURCES="imdb,tmdb,fandom,bravo"
DEFAULT_LABEL_PREFIX="trr.gallery.repair.stage3"
DEFAULT_OUTPUT_DIR="/tmp"
DEFAULT_STALE_MINUTES="240"

apply_flag=0
sources="${DEFAULT_SOURCES}"
label_prefix="${DEFAULT_LABEL_PREFIX}"
output_dir="${DEFAULT_OUTPUT_DIR}"
python_bin="${DEFAULT_PYTHON}"
stale_minutes="${DEFAULT_STALE_MINUTES}"
declare -a passthrough_args=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --apply)
      apply_flag=1
      shift
      ;;
    --sources)
      sources="${2:-}"
      shift 2
      ;;
    --label-prefix)
      label_prefix="${2:-}"
      shift 2
      ;;
    --output-dir)
      output_dir="${2:-}"
      shift 2
      ;;
    --python)
      python_bin="${2:-}"
      shift 2
      ;;
    --stale-minutes)
      stale_minutes="${2:-}"
      shift 2
      ;;
    --)
      shift
      passthrough_args+=("$@")
      break
      ;;
    *)
      passthrough_args+=("$1")
      shift
      ;;
  esac
done

if [[ ! -x "${python_bin}" ]]; then
  echo "error: python interpreter not executable: ${python_bin}" >&2
  exit 1
fi

mkdir -p "${output_dir}"
timestamp="$(date +%Y%m%d-%H%M%S)"
label="${label_prefix}.${timestamp}"
log_file="${output_dir}/gallery-host-repair-${timestamp}.log"
json_file="${output_dir}/gallery-host-repair-${timestamp}.json"
checkpoint_file="${output_dir}/gallery-host-repair-${timestamp}.checkpoint.json"
runner_file="${output_dir}/gallery-host-repair-${timestamp}.runner.sh"

{
  echo "#!/usr/bin/env bash"
  echo "set +e"
  echo "exec >> \"${log_file}\" 2>&1"
  echo "echo \"[start] \$(date +\"%Y-%m-%dT%H:%M:%S%z\")\""
  echo "cd \"${REPO_ROOT}\" || { echo \"[fatal] cd_failed\"; exit 1; }"
  echo "\"${python_bin}\" -u \"${REPO_ROOT}/scripts/media/repair_gallery_hosts.py\" \\"
  if [[ "${apply_flag}" -eq 1 ]]; then
    echo "  --apply \\"
  fi
  echo "  --sources \"${sources}\" \\"
  echo "  --output-json \"${json_file}\" \\"
  echo "  --checkpoint-file \"${checkpoint_file}\" \\"
  echo "  --resume-from-checkpoint \\"
  echo "  --force-flush-progress \\"
  for arg in "${passthrough_args[@]}"; do
    printf '  %q \\\n' "${arg}"
  done
  echo ""
  echo "rc=\$?"
  echo "echo \"[exit] \${rc} \$(date +\"%Y-%m-%dT%H:%M:%S%z\")\""
  echo "exit \${rc}"
} > "${runner_file}"

chmod +x "${runner_file}"

launchctl submit -l "${label}" -- /bin/bash "${runner_file}"
sleep 1

launchctl list | rg "${label}" >/dev/null

echo "LABEL=${label}"
echo "LOG=${log_file}"
echo "JSON=${json_file}"
echo "CHECKPOINT=${checkpoint_file}"
echo "RUNNER=${runner_file}"
echo "MONITOR_CMD=${python_bin} ${REPO_ROOT}/scripts/media/monitor_gallery_repair_run.py --label ${label} --log-path ${log_file} --json-path ${json_file} --checkpoint-path ${checkpoint_file} --stale-minutes ${stale_minutes}"
