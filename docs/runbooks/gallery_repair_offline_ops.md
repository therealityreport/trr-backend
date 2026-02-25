# Gallery Repair Offline Ops Runbook

## Purpose

Operate `scripts/media/repair_gallery_hosts.py` safely for long-running gallery host repair jobs.

## Standard Offline Launch

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
scripts/media/run_gallery_repair_offline.sh --apply --sources imdb,tmdb,fandom,bravo
```

Launcher output includes:
- `LABEL`
- `LOG`
- `JSON`
- `CHECKPOINT`
- `MONITOR_CMD`

## Live Monitoring

```bash
launchctl list | rg '<LABEL>'
tail -f <LOG>
python scripts/media/monitor_gallery_repair_run.py \
  --label <LABEL> \
  --log-path <LOG> \
  --json-path <JSON> \
  --checkpoint-path <CHECKPOINT> \
  --stale-minutes 240
```

Monitor states:
- `healthy/running`: run is active and log is not stale.
- `running-with-errors`: run is active and checkpoint summary shows non-zero `error`.
- `stalled`: run is active but log mtime exceeded stale threshold and no completed artifact.
- `completed-pass`: summary JSON exists with `summary.error == 0`.
- `completed-fail`: non-zero summary error, invalid/missing JSON, or run ended without successful artifact.

Monitor exit codes:
- `0` for `healthy/running` and `completed-pass`
- `3` for `running-with-errors`
- `2` for `stalled`
- `1` for `completed-fail`

## Hard Gate Validation (Completion)

```bash
python - <<'PY'
import json
from pathlib import Path

p = Path("<JSON>")
d = json.loads(p.read_text())
s = d.get("summary", {})
print(s)
assert s.get("apply") is True
assert int(s.get("scanned", 0)) > 0
assert int(s.get("error", 0)) == 0
print("SUMMARY_GATE=PASS")
PY
```

## Stall / Failure Triage

1. Stop run:
```bash
launchctl remove <LABEL> || true
```
2. Kill residual worker process if needed.
3. Run diagnostics:
```bash
python scripts/media/repair_gallery_hosts.py \
  --sources imdb,tmdb,fandom,bravo \
  --limit 100 \
  --output-json /tmp/gallery-host-repair-triage-100.json
```
4. Classify top reasons from `details` and record in handoff before relaunch.

## Relaunch Rule

Only relaunch apply after triage classification is recorded and reviewed.
