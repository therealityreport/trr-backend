# Cast Screen-Time Analytics — Acceptance Report

Repo: TRR-Backend
Last updated: 2026-03-16

## Current State
- `P0-P3` backend code paths are implemented and covered by targeted tests.
- Live control-plane proof now exists for:
  - episode upload -> verify -> run -> approve -> publish
  - promo upload -> verify -> run
  - stale-run recovery with unchanged persisted counts
  - standard workspace live-check wrapper execution with real env and assets
- A repeatable deployed smoke runner, stale-run drill, and cutover checklist now exist to collect the remaining evidence without manual API and SQL assembly.
- Official social-YouTube import now has live terminal success proof after two validation-found defects were fixed during this session.
- Hosted social sync-session schema drift is now cleared live outside the cast-screentime surface: `0193_youtube_asset_manifest_repair` is applied, `social.avatar_registry` exists, and the repaired schema passed a bounded live sync-session smoke.
- RHOSLC S6 historical mirror queue noise is now reduced: `3903` obsolete Threads `mirror_platform_not_supported:threads` failures were retired, leaving only targeted residual twitter/youtube follow-up instead of a fake full-queue blocker.
- A mistaken longer-runtime attempt was cancelled after validation showed the local asset was only `732.606875s` and not a real full episode; valid longer-runtime publish proof is still outstanding.

## P0 Gate Status
- `PASS` Direct-to-R2 upload contract exists and is validated in backend tests.
- `PASS` Verified upload is required before asset promotion and run creation.
- `PASS` Immutable manifest gating and worker finalization contracts are enforced by backend endpoints.
- `PASS` Live stale-run recovery validation now exists via `cast_screentime_stale_run_drill.py`; forced stale reconciliation marked the run failed and preserved persisted counts.
- `PASS` Live upload/run proof exists for episode and promo upload flows.
- `PASS` Live proof now exists for official social-YouTube import -> mirror -> asset promotion -> run success.
- `PENDING` Formal capture showing Render never proxies media bytes in the deployed path.

## P1 Gate Status
- `PASS` Backend supports artifact reads for reference-backed title-card and confessional review outputs.
- `PENDING` Golden Dataset quality proof for title-card matching and confessional thresholds.

## P2 Gate Status
- `PASS` Suggestion and unknown-review decision persistence exists.
- `PASS` Admin mutation endpoints for suggestion and unknown-review decisions exist.
- `PENDING` Live operator workflow proof across reruns and cross-run memory.

## P3 Gate Status
- `PASS` Episode-only canonical publish/version path exists.
- `PASS` Canonical show/season rollups exclude promo assets.
- `PASS` Publish-time reference fingerprint ingestion exists.
- `PASS` Live episode publish proof exists for run `6a92aceb-6fcc-43b4-b85f-77cda5fd58b8` -> publish version `1`.
- `PENDING` Publish/rollup proof with a longer real episode asset beyond the short uploaded demo clip.
- `PENDING` Operational observability review in deployed runtime.

## Repeatable Command
- `make cast-screentime-gap-check`
- `make cast-screentime-live-check`
- `python scripts/ops/cast_screentime_deployed_smoke.py ...`
- `python scripts/ops/cast_screentime_stale_run_drill.py ...`

## Remaining Operational Closeout
- Capture a deployed-path proof artifact showing media bytes go directly to object storage rather than through Render.
- Run the longer episode asset publish/rollup proof again with a real full-episode source, not a local Bravo download clip.

## Live Evidence Captured
- `PASS` `make cast-screentime-gap-check`:
  - backend pytest slice: `12 passed`
  - screenalytics pytest slice: `16 passed`
  - app proxy vitest slice: `4 passed`
  - Golden Dataset manifest: `5/5` baseline cases passed
- `PASS` `make cast-screentime-live-check` now runs real configured scenarios instead of skipping:
  - episode upload/run
  - promo upload/run
  - official social-YouTube import/run
  - stale-run reconcile drill
- `PASS` Episode upload/publish smoke:
  - `video_asset_id=060bc12e-f336-4d82-91ef-027c2616679f`
  - `run_id=6a92aceb-6fcc-43b4-b85f-77cda5fd58b8`
  - `status=success`
  - `review_status=approved`
  - `publish_version=1`
- `PASS` Promo upload smoke:
  - `video_asset_id=3c4eb855-a136-4b8b-a79b-21b037b92c1f`
  - `run_id=16a91bca-ac54-49e6-baa3-d949c94b7e89`
  - `status=success`
  - `is_publishable=false`
- `PASS` Official social-YouTube import smoke:
  - `video_asset_id=555b04c1-8aa4-43d3-8ab8-1115bbab5500`
  - `run_id=95cd7bbe-e903-4f5e-8bf8-f368b087a2e5`
  - `status=success`
  - `source_import_type=social_youtube_import`
  - `is_publishable=false`
- `PASS` Stale-run drill:
  - forced run `62ff2c84-0235-4006-8be5-06b6f1d669c6` into `running` with expired heartbeat
  - reconcile endpoint marked it `failed` with `worker_heartbeat_expired`
  - `artifact_count`, `evidence_count`, `excluded_section_count`, and `metric_count` remained unchanged
- `INVALIDATED` Longer-runtime episode attempt:
  - upload session `033dbaf0-d4c2-4bc6-9e9e-4aa35be3389b`
  - `video_asset_id=e14fc66f-3815-45d0-ba21-4388afdcb43d`
  - `run_id=d4489a91-659a-4cd5-937a-bd628d7881e8`
  - source file `scripts/socials/youtube/output/bravo_downloads/Oj872yqEFz4.mp4`
  - probed runtime `732.606875s`
  - terminal status: `cancelled`
  - cancellation reason: `operator_cancelled_invalid_runtime_evidence`
- `FIXED DURING VALIDATION` Live social import defects:
  - backend evidence replace now dedupes duplicate `evidence_key` payload rows before insert
  - workspace screenalytics startup now passes `TRR_API_URL`, which was missing after restart
