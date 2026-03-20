# Status — Task 12 (Cast Screen-Time Analytics)

Repo: TRR-Backend
Last updated: 2026-03-20

## Handoff Snapshot
```yaml
handoff:
  include: true
  state: active
  last_updated: 2026-03-20
  current_phase: "backend control-plane work is complete; remaining closure is limited to fresh-session operator validation and longer-runtime proof capture"
  next_action: "Use the restarted local workspace to validate the mirrored social-week import path and collect any remaining fresh-session evidence; archive this task once that operator proof is captured or intentionally deferred"
  detail: self
```

## Phase Status

| Phase | Description | Status | Notes |
|---|---|---|---|
| 1 | Implementation | Complete | Backend P0 control-plane now includes ingest probe validation, optional stale-run sweeper wiring, reconciliation, and evidence public URLs |
| 2 | Review enrichment | Complete | Backend now serves worker-generated JSON artifacts and dispatches on-demand exact/timestamp segment clip generation with evidence persistence |
| 3 | Scene and suggestion review | Complete | No new backend code was needed; the existing artifact/admin contracts carried the new P2 scene, suggestion, and unknown-review artifacts once the migration was applied live |
| 4 | Promo/test assets | Complete | Backend now supports promo-classified direct uploads plus official YouTube, external URL, and social YouTube row imports with owner-scope resolution and non-publishable enforcement |
| 5 | Canonical publish and rollups | Complete | Backend now persists episode-only publish versions, show/season canonical rollups, and publish-time reference fingerprints while keeping promo assets independent |
| 6 | Gap-closure scaffolding and persisted review state | Complete | Backend now persists operator suggestion/unknown decisions, ingests title-card signatures into the published reference corpus, and ships repeatable acceptance-report scaffolding while deployed live smoke and curated dataset validation remain pending |
| 7 | Deployed closure tooling | Complete | Backend now includes a deployed smoke runner, stale-run drill, and cutover/rollback checklists |
| 8 | Live control-plane proof and defect closure | Complete | Episode publish smoke, promo upload smoke, social-YouTube import smoke, stale-run drill, Golden Dataset pass-through baselines, duplicate-evidence dedupe, and workspace `TRR_API_URL` startup wiring are now in place while longer full-episode runtime proof remains operational follow-up |

## Blockers
- None.

## Recent Activity
- 2026-03-13: Task scaffolding created.
- 2026-03-13: Added cast screentime schema migration, backend admin/internal routers, repository layer, dispatch client, and focused API tests.
- 2026-03-13: Added stale-run reconciliation in the backend repository plus an admin trigger route and coverage for the new lifecycle path.
- 2026-03-13: Enriched evidence reads with hosted public URLs so the app can render proof-frame previews without storage internals.
- 2026-03-13: Added ffprobe-based upload verification before promotion, persisted probe metadata/runtime onto `video_assets`, and wired an optional backend stale-run sweeper loop behind `CAST_SCREENTIME_STALE_SWEEPER_ENABLED`.
- 2026-03-15: Added admin artifact reads for `shots.json`, `title_card_candidates.json`, and `confessional_candidates.json` through the backend-owned cast screentime surface.
- 2026-03-15: Added on-demand exact/timestamp segment clip generation through the backend screenalytics client and persisted returned video evidence rows for app review playback.
- 2026-03-16: Applied `0181_cast_screentime_control_plane` to Supabase and verified `media_upload_sessions`, `cast_screentime_segments`, `cast_screentime_evidence`, `cast_screentime_excluded_sections`, and `cast_screentime_show_settings` now exist live.
- 2026-03-16: Reused the existing backend artifact/admin routes for the P2 scene, cast-suggestion, and unknown-review artifacts without broadening the public contract.
- 2026-03-16: Added promo/test asset classification to `video_assets` and `media_upload_sessions`, plus owner-scope-aware direct-upload and remote-import contracts for trailer and episode-teaser assets.
- 2026-03-16: Added official-channel enforcement for YouTube imports using configured social targets, plus support for mirroring from pasted YouTube URLs, arbitrary external URLs, and existing `social.youtube_videos` rows.
- 2026-03-16: Applied `cast_screentime_promo_assets` to Supabase and verified the migration is recorded live.
- 2026-03-16: Added `cast_screentime_publish_versions` and `cast_screentime_reference_fingerprints`, plus explicit publish, publish-history, and show/season rollup routes for episode assets only.
- 2026-03-16: Applied `cast_screentime_publish_and_flashbacks` to Supabase and verified the migration succeeded.
- 2026-03-16: Added persisted suggestion-decision and unknown-review-state tables plus backend-owned decision endpoints for episode, season, and show contexts.
- 2026-03-16: Extended publish-time reference ingestion to include title-card reference signatures in the shared fingerprint corpus and added backend logging around publish/rollup failures.
- 2026-03-16: Added acceptance reports and a workspace gap-check command so cast-screentime closure can be revalidated consistently across backend, worker, and app repos.
- 2026-03-16: Added a deployed smoke runner for upload/import/run/approve/publish paths, plus explicit deployed-validation and cutover/rollback checklists tied to the app feature flag.
- 2026-03-16: Added a stale-run drill script and a workspace live-check wrapper so the remaining deployed closure steps can be executed with standard commands instead of manual API/SQL steps.
- 2026-03-16: Captured live episode upload -> approve -> publish proof for run `6a92aceb-6fcc-43b4-b85f-77cda5fd58b8` and fresh promo upload proof for run `16a91bca-ac54-49e6-baa3-d949c94b7e89`.
- 2026-03-16: Ran the stale-run drill against promo run `62ff2c84-0235-4006-8be5-06b6f1d669c6`; reconciliation failed the run with `worker_heartbeat_expired` while keeping persisted counts unchanged.
- 2026-03-16: Fixed live social-import validation regressions by deduping duplicate evidence keys during replace inserts and wiring `TRR_API_URL` through workspace screenalytics startup on restart.
- 2026-03-16: Captured live official social-YouTube import proof for run `95cd7bbe-e903-4f5e-8bf8-f368b087a2e5` after those runtime fixes, closing the missing third source-class smoke path.
- 2026-03-16: Executed `make cast-screentime-live-check` with real env/asset inputs; the wrapper now runs episode upload, promo upload, social-YouTube import, and stale-run reconciliation in one pass without skipping.
- 2026-03-16: Executed `make cast-screentime-gap-check`; backend tests (`12 passed`), screenalytics tests (`16 passed`), app proxy tests (`4 passed`), and Golden Dataset baselines (`5 passed`) all succeeded in one wrapper run.
- 2026-03-16: Cleared the hosted social sync-session blocker outside TASK12 by terminating stale Supavisor backend pid `3611221`, applying `0193_youtube_asset_manifest_repair`, and proving a live bounded sync-session smoke (`sync_session_id=d96b5ba5-2bf4-4563-b09f-c75900080227`, `run_id=7b0f3a70-4494-4c3b-b510-a283110c0715`) against the repaired schema.
- 2026-03-16: Retired `3903` obsolete RHOSLC S6 Threads `mirror_platform_not_supported:threads` failures via `scripts/socials/retire_stale_threads_media_mirror_failures.py --season-id e9161955-6ee4-4985-865e-3386a0f670fb --apply`, reducing residual mirror follow-up to narrow twitter/youtube slices instead of a false full-queue blocker.
- 2026-03-16: Attempted a longer-runtime validation with local file `scripts/socials/youtube/output/bravo_downloads/Oj872yqEFz4.mp4`, then invalidated it after confirming the asset is only `732.606875s` and not a real full episode. Upload session `033dbaf0-d4c2-4bc6-9e9e-4aa35be3389b` promoted `video_asset_id=e14fc66f-3815-45d0-ba21-4388afdcb43d`; run `d4489a91-659a-4cd5-937a-bd628d7881e8` was cancelled with `error_message=operator_cancelled_invalid_runtime_evidence`. Longer-runtime episode proof is still outstanding.
- 2026-03-16: Added backend regression coverage proving `external_url` cast-screentime imports stay on the external-import path and do not trigger official-channel YouTube validation, then synced the local shared-secret env wiring for TRR-APP and TRR-Backend dev processes.
