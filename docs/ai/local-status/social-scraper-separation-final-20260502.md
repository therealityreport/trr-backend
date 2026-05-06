# Social Scraper Separation Final Notes

## Handoff Snapshot
```yaml
handoff:
  include: true
  state: recent
  last_updated: 2026-05-02
  current_phase: "all planned social scraper separation phases completed through Instagram-first persistence, handler registry, router package split, and ops thinning"
  next_action: "Plan a separate compatibility-wrapper deletion pass after route/test patch paths no longer need the legacy facade."
  detail: self
```

Date: 2026-05-02

Plan source: `/Users/thomashulihan/Projects/TRR/.plan-grader/social-scraper-separation-auth-repair-aligned-20260502-192328/REVISED_PLAN.md`

## Scope

Completed the separation by moving the social season analytics implementation under `trr_backend/socials/`, leaving the
historical repository import path as a compatibility alias, and then unpacking the largest public scraper/read-model
surfaces into canonical room modules.

This preserves old import and monkeypatch behavior because `trr_backend.repositories.social_season_analytics` resolves
to the same module object as `trr_backend.socials.social_season_analytics_impl`, while the moved public surfaces now
delegate to canonical room implementations.

## Line Counts

- Before extraction: `65114` lines in `trr_backend/repositories/social_season_analytics.py`.
- After extraction: `9` lines in `trr_backend/repositories/social_season_analytics.py`.
- Canonical implementation core after room unpacking: `59364` lines in
  `trr_backend/socials/social_season_analytics_impl.py`.
- Canonical account-catalog rooms: `2418` lines under `trr_backend/socials/account_catalog/`.
- Canonical analytics rooms: `3883` lines under `trr_backend/socials/analytics/`.

## Canonical Owners

- Legacy compatibility facade:
  `trr_backend/repositories/social_season_analytics.py`
- Canonical implementation:
  `trr_backend/socials/social_season_analytics_impl.py`
- Account catalog launch/progress/profile reads:
  `trr_backend/socials/account_catalog/catalog_launch.py`,
  `trr_backend/socials/account_catalog/catalog_progress.py`,
  `trr_backend/socials/account_catalog/profile_reads.py`
- Analytics/read models/exports:
  `trr_backend/socials/analytics/read_models.py`
- Control-plane package surface:
  `trr_backend/socials/control_plane/`
- Existing Scrapling lane owners, unchanged:
  `trr_backend/socials/instagram/comments_scrapling/`,
  `trr_backend/socials/instagram/posts_scrapling/`,
  `trr_backend/socials/tiktok/posts_scrapling/`,
  `trr_backend/socials/threads/posts_scrapling/`

Internal control-plane/dashboard imports for the extracted room surfaces now target canonical account-catalog and
analytics modules. The social API router is now a package at `api/routers/socials/` with surface manifests for profiles,
catalog, season ingest, worker health, analytics, Reddit, and legacy scrape routes. Endpoint implementations remain in
`api/routers/socials/__init__.py` as the final compatibility facade for existing route patch paths.

## Compatibility Guardrails

- `tests/repositories/test_social_control_plane_imports.py` asserts that the legacy repository path aliases the
  canonical socials module.
- The same guard asserts that control-plane modules no longer import the repository compatibility path directly.
- Existing control-plane owner assertions verify extracted queue, run-read, shared-status, run-lifecycle, and
  dispatch-runtime surfaces.
- The guard now also verifies canonical ownership for catalog launch, catalog progress, profile reads, and analytics.

## Room Unpack Pass

New room modules own these public surfaces:

- Catalog launch: `start_social_account_catalog_backfill`, `begin_social_account_catalog_backfill_launch`,
  `finalize_social_account_catalog_backfill_launch`, `launch_social_account_catalog_backfill`.
- Catalog progress: `get_social_account_catalog_run_progress`.
- Profile reads: `get_social_account_profile_summary`, `get_social_account_profile_posts`,
  `get_social_account_profile_comments`, `get_social_account_profile_hashtags`,
  `get_social_account_profile_collaborators_tags`.
- Analytics/read models/exports: `get_week_live_health_snapshot`, `get_analytics`, `get_comments_coverage`,
  `get_mirror_coverage`, `get_week_detail`, `get_week_detail_summary_fast`, `get_week_detail_summary`, TikTok read
  models, `get_post_comments`, `build_csv`, `build_pdf`, and `pdf_filename`.

Private helper dependencies remain in `social_season_analytics_impl.py` where moving them would require broader
runtime/control-plane surgery. The room modules bridge to those helpers so route payloads, stage names, job metadata,
DB writes, and Scrapling lane behavior stay unchanged.

## Completion Pass

Completed the remaining planned phases after the Instagram comments auth-repair alignment:

- Control-plane worker health now has canonical implementations in `trr_backend/socials/control_plane/worker_health.py`
  for queue enablement, worker capabilities, remote auth probes, heartbeats, local trusted-worker reads, lane health, and
  worker availability assertions.
- Instagram post and comment persistence now lives in `trr_backend/socials/instagram/persistence.py`, with legacy core
  wrappers preserving old imports for `_upsert_instagram_post`, `_upsert_instagram_comment_tree`, and
  `_batch_upsert_instagram_comments`.
- Instagram catalog ingest, comments, auth runtime, and post-control rooms now sync patched legacy helpers at room
  boundaries so existing tests, route monkeypatches, and runtime callers keep the same behavior after ownership moves.
- `PlatformJobHandler` registry ownership is active in `trr_backend/socials/pipelines/job_handlers.py` with Instagram,
  TikTok, and Threads handler registrations.
- `scripts/socials/refresh_cookies.py` and `scripts/socials/instagram/direct_catalog_backfill.py` are thin CLI wrappers
  around reusable `trr_backend/socials/ops/*` package functions.
- Deferred Instagram comments follow-ups still launch through the guarded comments pipeline, preserving the launch-time
  comments endpoint probe and headed repair metadata contract.

## Test Alignment

Updated stale expectations to match current scraper behavior without changing scraper runtime contracts:

- Instagram comments Scrapling max attempts default is `12`.
- Incomplete Instagram full-history catalog backfills use `newest_first_frontier`.
- Stage-specific stale-timeout SQL shifts the null job-id filter params to the end of the query params.
- Instagram detail-rollup test clears relation-column cache before asserting cursor labels.
- Comments Scrapling cancellation test fixture now reflects the current lease query shape and final job-row load.

## Validation

Final completion-pass validation:

- `pytest -q tests/repositories/test_social_season_analytics.py` (`745 passed`)
- `pytest -q tests/repositories/test_social_control_plane_imports.py tests/repositories/test_social_run_lifecycle_repository.py tests/repositories/test_social_run_reads_repository.py tests/repositories/test_social_queue_status.py tests/repositories/test_social_dispatch_stage_claims.py` (`55 passed`)
- `pytest -q tests/api/routers/test_socials_route_shape.py tests/api/routers/test_socials_season_analytics.py` (`231 passed`)
- `pytest -q tests/socials/instagram/comments_scrapling/test_job_runner_cancellation.py tests/socials/test_instagram_comments_scrapling.py tests/socials/test_instagram_comments_scrapling_retry.py tests/socials/test_cookie_refresh_flows.py tests/socials/test_instagram_auth_resolver.py` (`167 passed`)
- `pytest -q tests/repositories/test_instagram_posts_scrapling_start.py tests/repositories/test_tiktok_posts_scrapling_start.py tests/repositories/test_instagram_comments_scrapling_lane.py tests/repositories/test_instagram_posts_scrapling_worker_lane.py tests/repositories/test_tiktok_posts_scrapling_worker_lane.py tests/repositories/test_instagram_comment_identity_contract.py` (`25 passed`, `1 skipped` because live test DB is unavailable)
- `pytest -q tests/api/routers/test_socials_reddit_refresh_routes.py tests/api/routers/test_socials_twitter_admin_routes.py tests/api/routers/test_twitter_persist_endpoint.py tests/api/routers/test_socials_tiktok_preview.py tests/api/routers/test_socials_tiktok_scrape.py tests/api/routers/test_socials_facebook.py` (`49 passed`)
- `pytest -q tests/scripts/test_refresh_social_cookies.py tests/scripts/test_instagram_comments_worker.py tests/scripts/test_verify_shared_account_catalog.py` (`11 passed`)
- `pytest -q tests/scripts/test_refresh_social_cookies.py tests/scripts/test_social_worker.py -k "heartbeat or cancelled_runs or caps"` (`5 passed`, `30 deselected`)
- `python -m compileall -q api/routers/socials trr_backend/socials scripts/socials tests/api/routers/test_socials_route_shape.py tests/repositories/test_social_control_plane_imports.py`
- `ruff check api/routers/socials trr_backend/socials/ops trr_backend/socials/instagram/catalog_ingest.py trr_backend/socials/instagram/auth_runtime.py trr_backend/socials/pipelines/comments/instagram.py trr_backend/socials/control_plane/run_lifecycle.py trr_backend/socials/social_season_analytics_impl.py scripts/socials/refresh_cookies.py scripts/socials/instagram/direct_catalog_backfill.py tests/api/routers/test_socials_route_shape.py tests/repositories/test_social_control_plane_imports.py`

- `python -m compileall -q trr_backend/repositories/social_season_analytics.py trr_backend/socials`
- `ruff check trr_backend/repositories/social_season_analytics.py trr_backend/socials/social_season_analytics_impl.py trr_backend/socials/account_catalog trr_backend/socials/analytics trr_backend/socials/control_plane tests/repositories/test_social_control_plane_imports.py`
- `pytest -q tests/repositories/test_social_control_plane_imports.py`
- `pytest -q tests/repositories/test_social_season_analytics.py tests/api/routers/test_socials_season_analytics.py`
- `pytest -q tests/repositories/test_social_run_lifecycle_repository.py tests/repositories/test_social_run_reads_repository.py tests/repositories/test_social_queue_status.py tests/repositories/test_social_dispatch_stage_claims.py tests/scripts/test_social_worker.py`
- `pytest -q tests/socials/instagram/comments_scrapling tests/socials/instagram/posts_scrapling tests/repositories/test_instagram_comments_scrapling_lane.py tests/repositories/test_instagram_posts_scrapling_lane.py tests/repositories/test_tiktok_posts_scrapling_lane.py tests/repositories/test_threads_posts_scrapling_lane.py`
- `pytest -q tests/repositories/test_social_control_plane_imports.py tests/socials/test_cookie_refresh_flows.py`
- `pytest -q tests/repositories/test_social_dispatch_stage_claims.py tests/socials/instagram/comments_scrapling/test_job_runner_cancellation.py`
- `pytest -q tests/api/routers/test_admin_scrape_contracts.py::test_import_images_stream_includes_operation_contract_fields`
- `pytest -q tests/api/routers/test_admin_show_links.py -k cleanup_invalid_show_knowledge_links`
- `pytest -q tests/socials/test_socialblade_scraper.py tests/api/test_admin_socialblade.py`
