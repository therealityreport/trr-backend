# Verification — Task 26

Repo: TRR-Backend
Last updated: 2026-04-04

## Acceptance Mapping

1. Worker-health and catalog-progress surfaces emit structured alert codes for rollout decisions.
Status: Complete in code and targeted tests.
Evidence:
- [social_season_analytics.py](/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/social_season_analytics.py) adds worker-health alerts, queue alerts, catalog-progress alerts, and shared-account backfill readiness payloads.
- [socials.py](/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/socials.py) exposes additive `shared_account_backfill_readiness` and `alerts` fields on the health-dot route.
- Targeted repository tests covering readiness and alert payloads passed.

2. Shared-profile payloads expose `network_name` without changing route shapes.
Status: Complete in code and targeted tests.
Evidence:
- [social_season_analytics.py](/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/social_season_analytics.py) adds `network_name` to shared-profile summary and catalog-progress payloads.
- Targeted repository tests covering shared-profile metadata passed.

3. Operator docs and templates no longer default to `bravotv` as the generic shared-profile example.
Status: Complete in docs.
Evidence:
- [.env.example](/Users/thomashulihan/Projects/TRR/TRR-Backend/.env.example) uses `network_handle` for Instagram cookie validation.
- [social_worker_queue_ops.md](/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/runbooks/social_worker_queue_ops.md) documents generic shared-profile rollout gates and alert interpretation.
- Existing Task 26 status already records the operator-template cleanup on 2026-04-02.

4. Targeted backend verification passes, live Modal readiness is confirmed, and rollout state is recorded in Task 26 docs.
Status: Partial. The frontier/auth fix is deployed and now proven live, but the Instagram authenticated session is still checkpointed so the replay cannot yet complete successfully.
Evidence:
- Targeted backend verification passed.
- Live Modal readiness verification passed on 2026-04-04.
- The updated backend was deployed successfully to Modal on 2026-04-04.
- Live worker-health now passes from this host using the correct service-role token shape for backend auth.
- A new bounded canary run on `instagram/bravotv` now fails closed with `frontier_auth_blocked` and `instagram_graphql_checkpoint_required`, which proves the deployed fix replaced the old generic cursor-request failure behavior.
- Both active Instagram frontier runs now show `instagram_graphql_cursor_request_failed` with frontier metadata `auth_allowed=false` and `auth_reason=checkpoint_required`.
- The local backend fix now converts that condition into a deterministic frontier auth failure (`frontier_auth_blocked` plus `instagram_graphql_checkpoint_required`) and marks aggregate run state as failed instead of leaving the replay in a misleading retry-only fetch state.
- Canonical cookie validation still reports `checkpoint_required`, and a forced refresh attempt from this environment did not recover a usable cookie bundle.
- The replay run therefore cannot be marked green until the Instagram authenticated session is repaired and replay is rerun.

## Code And Test Audit

Targeted files audited:
- [socials.py](/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/socials.py)
- [social_season_analytics.py](/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/social_season_analytics.py)
- [test_socials_season_analytics.py](/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_socials_season_analytics.py)
- [test_social_season_analytics.py](/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/repositories/test_social_season_analytics.py)

Verified behavior in the current workspace:
- Modal-required platforms fail closed for season ingest, sync sessions, shared ingest, and catalog-supported backfills.
- Bounded-window catalog backfills normalize midnight `date_end` values to inclusive end-of-day before dispatch.
- Catalog route messaging no longer claims an Instagram-only Modal requirement when the policy is broader.
- Instagram shared-account frontier runs now fail closed when auth health is already blocked (`checkpoint_required`) instead of attempting another cursor fetch and reporting a generic retryable cursor-request failure.
- Catalog progress now emits a deterministic `frontier_auth_blocked` alert and reports aggregate `run_state="failed"` when frontier auth is blocked, even if follow-on classify work is still present.
- Runtime-version drift detection now reads both top-level `metadata.runtime_version` and nested `metadata.retrieval_meta.runtime_version` values from job metadata.

Validation run:
- `ruff check api/routers/socials.py trr_backend/repositories/social_season_analytics.py tests/api/routers/test_socials_season_analytics.py tests/repositories/test_social_season_analytics.py`
- `ruff format --check api/routers/socials.py trr_backend/repositories/social_season_analytics.py tests/api/routers/test_socials_season_analytics.py tests/repositories/test_social_season_analytics.py`
- `pytest -q tests/api/routers/test_socials_season_analytics.py -k 'post_social_account_catalog_backfill_forwards_bounded_window_dates or post_social_account_catalog_backfill_tiktok or post_social_account_catalog_backfill_additional_supported_platforms or modal_required_platforms_and_queue_disabled or post_shared_ingest_requires_modal_when_queue_disabled or sync_session or ingest_requires_remote_worker_for_instagram_even_with_inline_fallback_enabled or ingest_comments_only_inline_fallback_spawns_per_platform_workers'`
  Result: `19 passed`
- `pytest -q tests/repositories/test_social_season_analytics.py -k 'start_social_account_catalog_backfill_requires_modal_executor or start_social_account_catalog_backfill_normalizes_midnight_end_date or assert_worker_available_when_queue_enabled_modal or build_modal_executor_health_payload or week_detail_instagram_includes_thumbnail_url or week_detail_tiktok_includes_thumbnail_url or week_detail_youtube_uses_effective_saved_comment_count or week_detail_facebook_includes_token_fallbacks or week_detail_threads_includes_token_fallbacks or week_detail_twitter_user_avatar_uses_raw_data_fallback'`
  Result: `17 passed`
- `pytest -q tests/socials/test_comment_scraper_fixes.py -k 'youtube_scrape_backfills_channel_avatar_and_title_from_channel_metadata or youtube_enrich_via_ytdlp_backfills_duration_when_missing or youtube_enrich_via_ytdlp_backfills_shorts_likes_from_html or instagram_scrape_graphql_backfills_owner_avatar_from_profile_payload or tiktok_scrape_backfills_post_avatar_from_user_detail or youtube_scrape_keeps_paging_through_too_recent_no_hit_pages or youtube_scrape_backfills_sparse_video_identity_fields_without_crashing'`
  Result: `7 passed`
- `pytest -q tests/repositories/test_social_season_analytics.py -k 'checkpoint_required or runtime_version_drift or frontier_auth_blocked or classify_backlog_after_scrape or frontier_lease_stale or dispatch_blocked or resume_frontier_cursor'`
  Result: `4 passed`
- `pytest -q tests/api/routers/test_socials_season_analytics.py -k 'catalog_run_progress or worker_health or shared_account_backfill_readiness'`
  Result: `4 passed`

## Live Modal Readiness Verification

Command run:
- `/Users/thomashulihan/Projects/TRR/TRR-Backend/.venv/bin/python scripts/modal/verify_modal_readiness.py --json`

Result:
- `ok: true`
- `app_name: trr-backend-jobs`
- `api_web_url: https://admin-56995--trr-backend-api.modal.run`
- no missing functions
- no missing secrets
- no missing web endpoints

Resolved social rollout-critical functions:
- `serve_backend_api`
- `run_social_job`
- `sweep_social_dispatch_queue`
- `heartbeat_remote_executors`
- `run_reddit_refresh`

## Live Rollout Execution Evidence

Authenticated worker-health:
- live `GET /api/v1/admin/socials/ingest/worker-health` returned `200`
- `queue_enabled=true`
- `healthy=true`
- `healthy_workers=4`
- `remote_auth_capabilities.instagram.ready=true`
- live dispatcher heartbeat showed `execution_backend_canonical=modal`

Current-environment admin access:
- live `GET /api/v1/admin/socials/ingest/worker-health` now returns `200` from this host when called with a correctly shaped service-role token
- inference: the access blocker was token shape, not the allowlist policy itself

Database-backed dispatcher health:
- `social.scrape_workers` still shows `modal:social-dispatcher` fresh at `2026-04-04 11:26:17+00`
- dispatcher metadata still reports `dispatch_enabled=true`, `execution_backend_canonical=modal`, and authenticated Instagram/TikTok/Facebook/Threads/Twitter capabilities
- inference: the primary live failure layer moved below dispatcher readiness into the Instagram frontier/auth path

Bounded canary acceptance:
- `POST /api/v1/admin/socials/profiles/instagram/bravotv/catalog/sync-recent` returned `409 SOCIAL_ACCOUNT_CATALOG_RUN_ALREADY_ACTIVE`
- the active run was then confirmed directly via the data plane:
  - `run_id=cecf6ad0-a849-4129-b132-79a129ae8eb6`
  - `status=completed`
  - `completed_at=2026-04-04 11:09:50+00`
  - single `shared_account_posts` job completed on Modal worker `modal:social:modal:2:bbfa3e30`
  - no failed jobs and no recorded dispatch error

Resume-tail evidence:
- active follow-on run `dc67440d-6daa-432a-a6ae-c972bbb7bee7` remains `running`
- `shared_account_discovery` completed after `history_bootstrap_resume`
- `shared_account_posts` failed at `2026-04-04 11:25:27+00` with:
  - `last_error_code=instagram_graphql_cursor_request_failed`
  - `error_message=Shared-account frontier fetch failed for @bravotv: instagram_graphql_cursor_request_failed`
  - runtime image `im-E7fb7cXjTujYaYPsnUkrCK`
- frontier state for the same run is now:
  - `status=retrying`
  - `retry_count=2`
  - `auth_allowed=false`
  - `auth_reason=checkpoint_required`
  - `resumed_cursor=3862810330682757561_2554414`
- the run also spawned `post_classify`, so the canary sequence is not cleanly green even though the initial bounded sync completed

Deployed-fix proof:
- bounded live canary run `f6bd45bc-cc2a-4fdf-9f67-7060912d39de` for `instagram/bravotv` now shows:
  - discovery succeeded (`33` checked / `33` saved)
  - frontier status `failed`
  - frontier metadata `auth_allowed=false`, `auth_reason=checkpoint_required`
  - frontier metadata `last_error_code=instagram_graphql_checkpoint_required`
  - alert `frontier_auth_blocked`
  - recent log line `Shared-account frontier auth blocked for @bravotv: checkpoint_required`
- inference: the deployed backend now surfaces the real auth failure directly instead of collapsing it into the old `instagram_graphql_cursor_request_failed` symptom

Affected replay launch:
- attempted live `POST /api/v1/admin/socials/profiles/instagram/bravodailydish/catalog/backfill` with `backfill_scope=full_history`
- the HTTP request timed out before a structured response was returned, but the replay run was created successfully in the database:
  - `run_id=360380f8-34ef-428b-96d7-0a507e480565`
  - run `status=queued` with `started_at=2026-04-04 11:13:55+00`
  - discovery job `b7de6c4d-83e5-4910-ac77-a6e208bcad03` completed on image `im-P9Hyfpnsr8Y0ARBmwL4MKl`
  - frontier job `60d0906f-b7fb-4766-89f5-d3fa43cdab3f` is now `retrying` with:
    - `last_error_code=instagram_graphql_cursor_request_failed`
    - `error_message=Shared-account frontier fetch failed for @bravodailydish: instagram_graphql_cursor_request_failed`
    - runtime image `im-E7fb7cXjTujYaYPsnUkrCK`
  - replay frontier state is now:
    - `status=retrying`
    - `retry_count=1`
    - `auth_allowed=false`
    - `auth_reason=checkpoint_required`
    - `graphql_cursor=3801988809270650791_19391525128`
  - classify backlog has not drained:
    - one `post_classify` job is `retrying`
    - one `post_classify` job remains `queued`
  - one retrying classify job also recorded `remote_blocked_reason=modal_capacity_pending`

Runtime version note:
- live run storage shows discovery jobs on image `im-P9Hyfpnsr8Y0ARBmwL4MKl` and later frontier/classify retries on image `im-E7fb7cXjTujYaYPsnUkrCK`
- inference: rollout evidence is consistent with runtime-version drift, even though the allowlisted `catalog/progress` surface could not be re-read directly from this host
- local code now surfaces this deterministically from job metadata even when the runtime label is nested under `retrieval_meta`

Instagram auth evidence:
- `scripts/socials/refresh_cookies.py --platform instagram --validate-only`
  - result: `validated=false`, `reason=checkpoint_required`
- `scripts/socials/refresh_cookies.py --platform instagram --force`
  - result: no recovered cookie bundle from this environment
- inference: the remaining blocker is real Instagram session health, not just stale run metadata

Password-rotation and fresh-session evidence:
- repaired the local Instagram browser session with the new password through the canonical Playwright path
- rotated Modal named secrets `trr-backend-runtime` and `trr-social-auth` with the same updated Instagram credential and fresh cookie payload
- redeployed `trr_backend.modal_jobs` after the secret rotation
- updated the local backend `.env` so `INSTAGRAM_PASSWORD` and `SOCIAL_AUTH_INSTAGRAM_*` no longer point at the stale password
- added a backend fallback so the canonical cookie refresh path also accepts legacy `INSTAGRAM_USERNAME` / `INSTAGRAM_PASSWORD`

Fresh bounded canary after password rotation:
- new bounded run `f214d5d4-7672-4422-90c3-8e9f082ed93f` for `instagram/bravotv` completed successfully
- final run summary:
  - `status=completed`
  - single `shared_account_posts` job completed
  - `failed_jobs=0`
- inference: the password/session repair was sufficient for a bounded recent-sync path

Fresh replay after password rotation:
- new full-history replay `aed0334a-2053-4f08-b624-7dff3c1807e7` for `instagram/bravodailydish` did not clear the deeper auth blocker
- replay sequence:
  - `shared_account_discovery` completed and saved the initial 33 posts
  - frontier then recorded `auth_allowed=false`, `auth_reason=checkpoint_required`, `next_cursor_present=true`
  - first `shared_account_posts` worker stalled and had to be recovered as a stale-heartbeat retry
  - retried `shared_account_posts` then failed explicitly with:
    - `last_error_code=instagram_graphql_checkpoint_required`
    - `error_message=Shared-account frontier auth blocked for @bravodailydish: checkpoint_required`
  - `post_classify` also accumulated stale-heartbeat retry noise
  - replay was cancelled after the auth failure was proven
- final replay run summary:
  - `status=cancelled`
  - `completed_jobs=1`
  - `failed_jobs=1`
  - `items_found_total=66`

Local reproduction against the fresh cookie bundle:
- direct local `InstagramScraper.fetch_posts_graphql('bravodailydish')` using `data/instagram_cookies.json` now fails immediately with:
  - `error_code=instagram_graphql_checkpoint_required`
  - `error_status_code=400`
  - `error_message=checkpoint_required`
- the canonical repo validation path now also returns:
  - `scripts/socials/refresh_cookies.py --platform instagram --validate-only`
  - `validated=false`, `reason=checkpoint_required`
- inference: the remaining blocker is not a stale backend deploy, stale local password, or stale Modal secret. The Instagram account is still checkpoint-gated for the authenticated GraphQL path needed by deeper replay pagination.

Operational note:
- the live `catalog/verification`, `catalog/runs/{run_id}/progress`, and some profile-summary reads can exceed a 30-second HTTP timeout while the backend is under load. For this execution pass, database-backed confirmation was required to verify the canary and replay state when the read surfaces did not respond in time.
- from the current host, admin allowlist gating is now the dominant blocker rather than request timeout. Database-backed confirmation remains the only feasible source of truth available from this environment until allowlist access is restored.
- the aggregate `social.scrape_runs.status` field can lag active frontier/job state. For these live replay diagnoses, `social.shared_account_run_frontiers` and `social.scrape_jobs` were the stronger source of truth than the top-level run row alone.

## Remaining Acceptance Gap

The remaining gap is now external Instagram checkpoint clearance plus rerun:
- clear the Instagram checkpoint in a way that restores authenticated GraphQL access beyond the first public discovery page
- rerun the affected full-history replay after checkpoint clearance
- capture fresh control-plane progress evidence showing the frontier no longer flips to `auth_reason=checkpoint_required`
- confirm whether mixed Modal image labels still appear after the replay truly clears; if they do, keep `runtime_version_drift` open as a real rollout issue

Task 26 should remain active. The live rollout now proves both things that mattered: the backend fix works, and the remaining blocker is the real Instagram session checkpoint state.
