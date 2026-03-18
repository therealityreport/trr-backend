# Cross-platform social sync-session final follow-through

Last updated: 2026-03-16

## Handoff Snapshot
```yaml
handoff:
  include: true
  state: active
  last_updated: 2026-03-16
  current_phase: "Threads sync-session kickoff repaired and live RHOSLC Week 2 button-path proof captured on Modal"
  next_action: "Monitor the live session follow-up passes and address only any remaining normal comments/media gaps, not infrastructure/runtime breakage"
  detail: self
```

- Threads remote workers now build from a shared social image base in [modal_jobs.py](/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/modal_jobs.py), with Playwright Chromium installed only on the browser-capable image and `run_social_job` explicitly bound to that browser image.
- Focused regression coverage now locks the shared social image payloads, browser install steps, and `run_social_job` image binding in [test_modal_jobs.py](/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/test_modal_jobs.py).
- Modal app `trr-backend-jobs` was redeployed on 2026-03-16 and `scripts/modal/verify_modal_readiness.py --json` reported `ok=true` with `run_social_job` resolved.
- Live validation against RHOSLC S6 Week 2 proved the original Threads browser failure is gone on the remote worker path:
  - pre-fix failed job `a3035806-cff0-4740-af66-b303d55c3f9d` died with `BrowserType.launch: Executable doesn't exist`
  - post-deploy Threads-only ingest run `0e09a193-54f2-47d9-b2b4-9343084742f1` queued on Modal with `execution_backend_canonical=modal`
  - at least one Threads `posts` job completed (`1773cd14-6255-4465-83c1-2f9357933be3`) with `retrieval_meta.source=playwright_profile_discovery`
  - the new run has `0` jobs failing with the old missing-executable signature
- Fixed the sync-session completeness-query drift in [social_sync_orchestrator.py](/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/social_sync_orchestrator.py) so Threads missing-comment targeting uses `replies_count + quotes` instead of the nonexistent `comments_count` column on `social.meta_threads_posts`.
- Added focused regression coverage for that sync-session path in [test_social_sync_orchestrator.py](/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/repositories/test_social_sync_orchestrator.py).
- Live button-path proof after redeploy:
  - `POST /api/v1/admin/socials/seasons/e9161955-6ee4-4985-865e-3386a0f670fb/sync-sessions` now returns `status=created` instead of the old SQL error
  - sync session `3331aff2-fac7-4134-aa0f-390f3eede83e` started `current_run_id=0f62a57d-369e-4d0a-ad71-ac39a839cbbb`
  - the run summary advanced into real work with Threads `posts` and `comments` jobs, and at least one Threads `posts` job completed (`2128bc30-c81a-418a-b3a7-f94ffbc6a0be`) on Modal with `retrieval_meta.source=playwright_profile_discovery`

- Added the backend sync-session SSE route and verified it emits additive live payloads for active sessions.
- Extended non-Instagram `details_refresh` so targeted repair runs can rehydrate details and enqueue media/comment-media repair work instead of stopping at stale metadata.
- Focused validation passed for the touched backend router/orchestrator/details-refresh paths.
- Week-detail status now resolves the latest week run per platform instead of picking a single run for the whole week, so RHOSLC Week 2 cards point at the correct week-scoped run ids for Instagram, TikTok, Twitter, YouTube, Facebook, and Threads.
- RHOSLC YouTube week detail no longer hides long-form Bravo videos behind the stricter `#RHOSLC` persisted-post filter; the live Week 2 window now shows all `11` stored YouTube posts (`5` long-form videos and `6` Shorts).
- Sync-session coverage payloads now normalize comment display counts when saved comments exceed stale post-level reported counts, so active TikTok sessions render `4573/4573` with the raw prior reported count preserved separately instead of showing an impossible `4573/4368`.
- TikTok week-detail cards now downgrade mirror-only failures to `partial` instead of hard `failed`, so RHOSLC Week 2 media follow-up remains visible without mislabeling the posts/comments pass as failed.
- TikTok fast week-summary comment totals now respect the lifecycle `is_missing = false` filter, matching the active-comment counting used elsewhere.
- YouTube Shorts likes enrichment now falls back to the Shorts page `#button-bar > reel-action-bar-view-model > like-button-view-model > toggle-button-view-model > button-view-model > label > div > span` branch when `yt-dlp` returns no `like_count`, so visible Shorts like counts can still be recovered from page HTML.
- Fixed the workspace `chrome-devtools-mcp` preflight wrapper regression by moving the wrapper-local `log()` helper before the stale-session cleanup sweep in [codex-chrome-devtools-mcp.sh](/Users/thomashulihan/Projects/TRR/scripts/codex-chrome-devtools-mcp.sh); stale isolated session cleanup no longer falls through to macOS `/usr/bin/log`.
- `make dev` now clears the browser-automation preflight again and the non-reload backend has been restarted successfully with the YouTube Shorts patch live in the local workspace.
- Remaining known issue: active sync-session payloads still serialize the last persisted completeness snapshot while the current run is `running`, so live run progress can exceed the displayed coverage snapshot until the pass is re-evaluated.
