# Cast Screen-Time Deployed Validation Runbook

Repo: TRR-Backend  
Last updated: 2026-03-16

## Goal
Run the original zero-trust closure checks against a deployed backend instead of relying on local-only unit coverage.

## Required Environment
- `TRR_API_URL`
- `TRR_CORE_SUPABASE_SERVICE_ROLE_KEY`
- `TRR_INTERNAL_ADMIN_SHARED_SECRET`

## Primary Script
- `python scripts/ops/cast_screentime_deployed_smoke.py ...`
- `python scripts/ops/cast_screentime_stale_run_drill.py ...`
- `make cast-screentime-live-check`

## Episode Upload Smoke
```bash
python scripts/ops/cast_screentime_deployed_smoke.py \
  --api-base-url "$TRR_API_URL" \
  --owner-scope episode \
  --owner-id "$EPISODE_ID" \
  --show-id "$SHOW_ID" \
  --season-id "$SEASON_ID" \
  --episode-id "$EPISODE_ID" \
  --video-class episode \
  --wait \
  upload-run \
  --video-file /absolute/path/to/test-episode-clip.mp4
```

## Promo Upload Smoke
```bash
python scripts/ops/cast_screentime_deployed_smoke.py \
  --api-base-url "$TRR_API_URL" \
  --owner-scope season \
  --owner-id "$SEASON_ID" \
  --show-id "$SHOW_ID" \
  --season-id "$SEASON_ID" \
  --video-class promo \
  --promo-subtype trailer \
  --wait \
  upload-run \
  --video-file /absolute/path/to/test-trailer.mp4
```

## YouTube Promo Import Smoke
```bash
python scripts/ops/cast_screentime_deployed_smoke.py \
  --api-base-url "$TRR_API_URL" \
  --owner-scope season \
  --owner-id "$SEASON_ID" \
  --show-id "$SHOW_ID" \
  --season-id "$SEASON_ID" \
  --video-class promo \
  --promo-subtype trailer \
  --wait \
  import-run \
  --source-mode youtube_url \
  --source-url "https://www.youtube.com/watch?v=..."
```

## Episode Publish Smoke
```bash
python scripts/ops/cast_screentime_deployed_smoke.py \
  --api-base-url "$TRR_API_URL" \
  --owner-scope episode \
  --owner-id "$EPISODE_ID" \
  --show-id "$SHOW_ID" \
  --season-id "$SEASON_ID" \
  --episode-id "$EPISODE_ID" \
  --video-class episode \
  --wait \
  --approve \
  --publish \
  upload-run \
  --video-file /absolute/path/to/test-episode-clip.mp4
```

## Required Evidence To Capture
- Response payload showing the presigned `put_url` for upload mode
- Terminal run payload for each smoke path
- For publish smoke, the returned publish-version payload
- Operator screenshot or saved JSON confirming promo assets remain non-canonical
- Stale-run drill evidence:
  - output from `cast_screentime_stale_run_drill.py`
  - run id
  - stale reconciliation result
  - before/after persisted counts remain unchanged
  - follow-up rerun evidence if a replay is executed separately

## Stale-Run Drill
```bash
python scripts/ops/cast_screentime_stale_run_drill.py \
  --api-base-url "$TRR_API_URL" \
  --run-id "$RUN_ID"
```

## Workspace Wrapper
- `make cast-screentime-live-check` wraps the deployed smoke and stale-run drill commands.
- It skips scenarios with missing environment variables instead of failing the whole runbook.

## Report Back To
- `ACCEPTANCE_REPORT.md`
- `STATUS.md`
