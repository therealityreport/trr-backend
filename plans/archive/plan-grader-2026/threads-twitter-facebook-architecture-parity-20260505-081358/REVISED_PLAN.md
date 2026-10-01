# Revised Implementation Plan: Threads, Twitter/X, and Facebook Architecture Parity

Date: 2026-05-05
Scope: `/Users/thomashulihan/Projects/TRR/TRR-Backend`
Source plan: `docs/codex/plans/2026-05-05-threads-twitter-facebook-architecture-parity.md`
Status: Ready for execution with `orchestrate-subagents`

## Summary

Improve Threads, Twitter/X, and Facebook social scraper architecture by carrying forward the platform-local Module patterns already used for Instagram, TikTok, and YouTube.

Target outcome:

- Threads claimed-job posts runs support cancellation, degraded DB finalization/read behavior, safe terminal metadata, and skip-reason counters.
- Twitter/X, Facebook, and Threads remote-auth readiness probes work for all platforms listed in `REMOTE_AUTH_REQUIRED_PLATFORMS`.
- Twitter/X, Facebook, and Threads shared-account catalog orchestration moves into platform-local `posts_catalog` Modules while legacy monolith functions remain compatibility wrappers.
- No DB schema, route payload, scrape stage, worker lane, comments contract, or app-facing contract changes are introduced.

## Current Repo Evidence

- `docs/architecture/social-platform-module-checklist.md` defines the expected platform Module shape.
- `trr_backend/socials/threads/posts_scrapling/` already owns a claimed-job lane with `session.py`, `proxy.py`, `fetcher.py`, `persistence.py`, and `job_runner.py`.
- `trr_backend/socials/threads/jobs.py` registers `threads_posts_scrapling`.
- `trr_backend/socials/threads/posts_scrapling/job_runner.py` lacks the cancellation and degraded finalization/read handling now expected from the hardened Instagram/TikTok lanes.
- `REMOTE_AUTH_REQUIRED_PLATFORMS` in `trr_backend/socials/social_season_analytics_impl.py` includes `twitter`, `facebook`, and `threads`, but the remote auth probe path does not yet support all three.
- Cookie loaders and validators already exist for Twitter/X, Facebook, and Threads, including `_cookie_platform_load_from_sources()`, `_cookie_platform_validate()`, `_load_twitter_auth_from_sources()`, `_validate_twitter_cookie_health()`, `_load_facebook_cookies_from_sources()`, `_validate_facebook_cookie_health()`, `_load_threads_cookies_from_sources()`, and `_validate_threads_cookie_health()`.
- `_scrape_shared_twitter_posts()`, `_scrape_shared_facebook_posts()`, and `_scrape_shared_threads_posts()` still own shared catalog orchestration in `social_season_analytics_impl.py`.
- `trr_backend/socials/twitter/` and `trr_backend/socials/facebook/` have scraper/auth/fetch support Modules but no `posts_catalog` Modules.
- The worktree is dirty. Executors must inspect diffs before editing and must preserve unrelated user changes.

## Non-Goals And Stop Rules

- Do not add DB tables, migrations, route payload fields, new scrape stages, new API routes, new app UI behavior, or new worker lanes.
- Do not create Twitter/X or Facebook claimed-job lanes in this pass. Their work is shared catalog Module extraction plus readiness hardening only.
- Do not merge `trr_backend/socials/threads/posts_scrapling/` and the new `trr_backend/socials/threads/posts_catalog/`. They are different seams.
- Do not move Twitter/X comments, quotes, media mirroring, or repair scripts unless a focused posts catalog test proves direct coupling.
- Do not move Facebook comments, share detail refresh, media mirror behavior, or `_upsert_facebook_comment_tree()` in this pass.
- Do not switch any of the three platforms to batch upsert unless equivalence tests prove payload shape, optional-column gates, conflict target, assignment payload, `job_id`, and return-shape behavior.
- Do not emit raw cookie, bearer token, CSRF token, twikit credential, or proxy secret values in readiness payloads, runtime metadata, fixtures, docs, or failures.
- Do not revert unrelated dirty worktree changes.

## Phase 0: Tests First

Add or update tests before behavior changes where feasible. Use exact node IDs where possible so workers can run focused checks without triggering unrelated broad failures.

### Threads Claimed-Job Lane Tests

File: `tests/socials/threads/posts_scrapling/test_job_runner.py`

Add these tests:

- `test_threads_job_runner_marks_cancelled_job`
- `test_threads_job_runner_marks_cancelled_run`
- `test_threads_job_runner_checks_cancellation_after_fetch`
- `test_threads_job_runner_returns_degraded_summary_when_final_read_saturated`
- `test_threads_job_runner_defers_run_finalization_when_db_saturated`
- `test_threads_job_runner_records_terminal_runtime_metadata`

Expected pre-fix failures:

- no lane-local cancellation helper exists,
- final `lifecycle.finalize_run_status(run_id)` is not degraded-DB safe,
- final `pg.fetch_one(...)` summary read is not degraded-DB safe,
- terminal metadata does not consistently include skip-reason and runtime/progress counters.

File: `tests/socials/threads/posts_scrapling/test_persistence.py`

Add or extend:

- `test_persist_threads_posts_counts_skips_by_reason`

Expected pre-fix failure:

- `PersistedThreadsPosts` does not expose a stable `posts_skipped_by_reason` result.

### Remote Auth Readiness Tests

File: `tests/repositories/test_social_season_analytics.py`

Add these tests:

- `test_probe_remote_auth_health_reports_twitter_safe_structure_flags`
- `test_probe_remote_auth_health_reports_facebook_safe_structure_flags`
- `test_probe_remote_auth_health_reports_threads_safe_structure_flags`
- `test_probe_remote_auth_health_rejects_unsupported_platform`

Expected pre-fix failures:

- Twitter/X, Facebook, and Threads are listed as remote-auth-required but are rejected or unsupported by the probe path.

File: `tests/scripts/test_verify_modal_readiness.py`

Add or extend:

- `test_parse_args_accepts_twitter_remote_auth_probe`
- `test_parse_args_accepts_facebook_remote_auth_probe`
- `test_parse_args_accepts_threads_remote_auth_probe`
- `test_remote_auth_probe_failure_blocks_strict_readiness`

Expected pre-fix failures:

- CLI choices do not accept all remote-auth-required platforms, or strict readiness does not surface probe failure consistently.

### Shared Catalog Delegation Tests

File: `tests/repositories/test_social_season_analytics.py`

Add these wrapper tests:

- `test_scrape_shared_twitter_posts_delegates_to_posts_catalog`
- `test_scrape_shared_facebook_posts_delegates_to_posts_catalog`
- `test_scrape_shared_threads_posts_delegates_to_posts_catalog`

Expected pre-fix failures:

- each compatibility wrapper still owns catalog orchestration instead of delegating to a platform-local Module.

Platform-local tests:

- add `tests/socials/twitter/test_posts_catalog.py`,
- add `tests/socials/facebook/test_posts_catalog.py`,
- add `tests/socials/threads/test_posts_catalog.py`.

Each should cover:

- one normal catalog row,
- one empty or retryable metadata case,
- no-network execution through fake scraper/auth/persistence dependencies,
- preservation of existing retrieval metadata names used by callers.

### Import And Queue Status Tests

File: `tests/repositories/test_social_control_plane_imports.py`

Add import coverage for:

- `trr_backend.socials.twitter.posts_catalog`
- `trr_backend.socials.facebook.posts_catalog`
- `trr_backend.socials.threads.posts_catalog`

File: `tests/repositories/test_social_queue_status.py`

Add or extend tests that prove new `last_error_code` values and readiness failures remain visible in queue status summaries without changing route payloads.

## Phase 1: Extend Remote Auth Readiness

Do this before large catalog extraction because it is a concrete operator-facing bug and it touches shared readiness code.

### Implementation

Files:

- `trr_backend/socials/social_season_analytics_impl.py`
- `scripts/modal/verify_modal_readiness.py`
- `tests/repositories/test_social_season_analytics.py`
- `tests/scripts/test_verify_modal_readiness.py`

Tasks:

1. Keep Instagram and TikTok remote-auth behavior unchanged.
2. Extend `probe_remote_auth_health(platform)` for `twitter`, `facebook`, and `threads`.
3. Prefer the existing cookie registry helpers where possible:
   - `_cookie_platform_load_from_sources(platform)`
   - `_cookie_platform_validate(platform, cookies)`
4. Use platform-specific loaders only where the registry does not cover needed safe structure flags:
   - Twitter/X: `_load_twitter_auth_from_sources()` plus `_validate_twitter_cookie_health()`.
   - Facebook: `_load_facebook_cookies_from_sources()` plus `_validate_facebook_cookie_health()`.
   - Threads: `_load_threads_cookies_from_sources()` plus `_validate_threads_cookie_health()`.
5. Add a small helper such as `_remote_auth_safe_structure_flags(platform, ...)`.
6. Return only safe booleans and validation summaries.

Required safe flags:

- Twitter/X: `has_auth_token`, `has_ct0`, `has_bearer_token`, `has_twikit_credentials`.
- Facebook: `has_c_user`, `has_xs`.
- Threads: `has_sessionid`, `has_csrftoken`.

CLI tasks:

1. Allow `--probe-remote-auth twitter`.
2. Allow `--probe-remote-auth facebook`.
3. Allow `--probe-remote-auth threads`.
4. Keep unsupported platform behavior explicit.
5. Preserve JSON output shape for existing Instagram/TikTok probes.

Validation:

```bash
pytest tests/repositories/test_social_season_analytics.py::test_probe_remote_auth_health_reports_twitter_safe_structure_flags \
  tests/repositories/test_social_season_analytics.py::test_probe_remote_auth_health_reports_facebook_safe_structure_flags \
  tests/repositories/test_social_season_analytics.py::test_probe_remote_auth_health_reports_threads_safe_structure_flags \
  tests/repositories/test_social_season_analytics.py::test_probe_remote_auth_health_rejects_unsupported_platform \
  tests/scripts/test_verify_modal_readiness.py::test_parse_args_accepts_twitter_remote_auth_probe \
  tests/scripts/test_verify_modal_readiness.py::test_parse_args_accepts_facebook_remote_auth_probe \
  tests/scripts/test_verify_modal_readiness.py::test_parse_args_accepts_threads_remote_auth_probe \
  tests/scripts/test_verify_modal_readiness.py::test_remote_auth_probe_failure_blocks_strict_readiness
```

## Phase 2: Harden Threads Posts Scrapling Lane

Files:

- `trr_backend/socials/threads/posts_scrapling/job_runner.py`
- `trr_backend/socials/threads/posts_scrapling/persistence.py`
- `trr_backend/socials/threads/posts_scrapling/fetcher.py`
- `tests/socials/threads/posts_scrapling/test_job_runner.py`
- `tests/socials/threads/posts_scrapling/test_persistence.py`

Tasks:

1. Add a lane-local cancellation exception.
2. Add `_raise_if_cancelled(job_id, run_id, ...)`, modeled on the hardened Instagram/TikTok posts lanes.
3. Check cancellation after:
   - session/auth resolution,
   - proxy/session warmup,
   - fetch,
   - persist,
   - before final completion.
4. Finish cancelled jobs with:
   - `last_error_code="threads_posts_scrapling_cancelled"`,
   - `cancel_scope`,
   - `job_status`,
   - `run_status`,
   - safe runtime metadata.
5. Catch `pg.DatabaseServiceUnavailableError` around `lifecycle.finalize_run_status(run_id)`.
6. Catch `pg.DatabaseServiceUnavailableError` around final `pg.fetch_one(...)`.
7. Return a degraded summary with stable fields when final DB reads are unavailable.
8. Extend `PersistedThreadsPosts` or returned diagnostics to include `posts_skipped_by_reason`.
9. Include terminal metadata for:
   - `listing_progress`,
   - `stage_counters`,
   - `persist_counters`,
   - `fetch_counters`,
   - `fetcher_runtime`,
   - `runtime_metadata`,
   - `source_runtime`,
   - `stop_reason`,
   - `activity`.
10. Preserve safe fingerprints only.
11. Keep legacy `ThreadsScraper` fallback only when the Scrapling fetcher fails with no posts and current retry policy allows fallback.

Validation:

```bash
pytest tests/socials/threads/posts_scrapling/test_job_runner.py::test_threads_job_runner_marks_cancelled_job \
  tests/socials/threads/posts_scrapling/test_job_runner.py::test_threads_job_runner_marks_cancelled_run \
  tests/socials/threads/posts_scrapling/test_job_runner.py::test_threads_job_runner_checks_cancellation_after_fetch \
  tests/socials/threads/posts_scrapling/test_job_runner.py::test_threads_job_runner_returns_degraded_summary_when_final_read_saturated \
  tests/socials/threads/posts_scrapling/test_job_runner.py::test_threads_job_runner_defers_run_finalization_when_db_saturated \
  tests/socials/threads/posts_scrapling/test_job_runner.py::test_threads_job_runner_records_terminal_runtime_metadata \
  tests/socials/threads/posts_scrapling/test_persistence.py::test_persist_threads_posts_counts_skips_by_reason
```

## Phase 3: Extract Twitter/X Posts Catalog Module

Files:

- new `trr_backend/socials/twitter/posts_catalog/__init__.py`
- new `trr_backend/socials/twitter/posts_catalog/catalog.py`
- `trr_backend/socials/social_season_analytics_impl.py`
- `tests/socials/twitter/test_posts_catalog.py`
- `tests/repositories/test_social_season_analytics.py`

Tasks:

1. Create `trr_backend/socials/twitter/posts_catalog/`.
2. Implement `scrape_shared_twitter_posts(...)` in `catalog.py`.
3. Use dependency injection for monolith helpers, persistence callbacks, auth access, and scraper access to avoid circular imports.
4. Preserve existing behavior:
   - full-history window policy,
   - page-limit policy,
   - reply filtering,
   - profile snapshot building,
   - `posts_checked`,
   - `pages_scanned`,
   - `persist_counters`,
   - `last_retrieval_meta`,
   - wrapper return shape `(rows, retrieval_meta)`.
5. Keep `_scrape_shared_twitter_posts()` as a compatibility wrapper.
6. Do not create new Twitter/X `jobs.py`, stage claims, queue lanes, route payloads, or worker dispatch cases.
7. Do not move `scripts/socials/twitter/scrape.py` persistence semantics.
8. Do not move Twitter/X comments, quotes, repair, or media mirror behavior.

Validation:

```bash
pytest tests/socials/twitter/test_posts_catalog.py \
  tests/repositories/test_social_season_analytics.py::test_scrape_shared_twitter_posts_delegates_to_posts_catalog \
  tests/socials/test_twitter_query_building.py \
  tests/socials/test_twitter_rate_limiting.py \
  tests/socials/test_twitter_runtime_metadata.py \
  tests/scripts/test_twitter_scrape_cli.py \
  tests/scripts/test_twitter_scrape_persist.py
```

## Phase 4: Extract Facebook Posts Catalog Module

Files:

- new `trr_backend/socials/facebook/posts_catalog/__init__.py`
- new `trr_backend/socials/facebook/posts_catalog/catalog.py`
- `trr_backend/socials/social_season_analytics_impl.py`
- `tests/socials/facebook/test_posts_catalog.py`
- `tests/repositories/test_social_season_analytics.py`

Tasks:

1. Create `trr_backend/socials/facebook/posts_catalog/`.
2. Implement `scrape_shared_facebook_posts(...)` in `catalog.py`.
3. Use dependency injection for scraper/document-fetch/persistence callbacks to avoid circular imports.
4. Preserve existing behavior:
   - `FacebookScrapeConfig` defaults,
   - delay env behavior,
   - max scroll env behavior,
   - profile snapshot building,
   - `last_retrieval_meta`,
   - shared catalog progress,
   - wrapper return shape.
5. Keep `_scrape_shared_facebook_posts()` as a compatibility wrapper.
6. Do not create a Facebook claimed-job lane, worker stage, route payload, or dispatch case.
7. Keep comments, share details, detail refresh, and media mirror follow-up out of this pass.

Validation:

```bash
pytest tests/socials/facebook/test_posts_catalog.py \
  tests/repositories/test_social_season_analytics.py::test_scrape_shared_facebook_posts_delegates_to_posts_catalog \
  tests/socials/test_facebook_document_fetch.py \
  tests/socials/test_facebook_engagement.py \
  tests/api/routers/test_socials_facebook.py \
  tests/repositories/test_social_mirror_repairs.py
```

## Phase 5: Extract Threads Shared Catalog Module

Files:

- new `trr_backend/socials/threads/posts_catalog/__init__.py`
- new `trr_backend/socials/threads/posts_catalog/catalog.py`
- `trr_backend/socials/social_season_analytics_impl.py`
- `tests/socials/threads/test_posts_catalog.py`
- `tests/repositories/test_social_season_analytics.py`

Tasks:

1. Create `trr_backend/socials/threads/posts_catalog/`.
2. Implement `scrape_shared_threads_posts(...)` in `catalog.py`.
3. Keep the new catalog Module separate from `threads/posts_scrapling`.
4. Preserve existing behavior:
   - `ThreadsScrapeConfig`,
   - env delay behavior,
   - profile snapshot building,
   - `last_retrieval_meta`,
   - shared catalog progress,
   - existing persistence callbacks,
   - wrapper return shape.
5. Keep `_scrape_shared_threads_posts()` as a compatibility wrapper.
6. Do not route shared catalog work through the claimed-job lifecycle lane.

Validation:

```bash
pytest tests/socials/threads/test_posts_catalog.py \
  tests/repositories/test_social_season_analytics.py::test_scrape_shared_threads_posts_delegates_to_posts_catalog \
  tests/socials/test_threads_scraper.py
```

## Phase 6: Operator Fixtures, Docs, And Final Validation

Files:

- `tests/fixtures/socials/run_metadata/`
- `docs/runbooks/social_worker_queue_ops.md`
- `docs/architecture/social-platform-module-checklist.md`
- `tests/repositories/test_social_control_plane_imports.py`
- `tests/repositories/test_social_queue_status.py`

Tasks:

1. Add representative metadata fixtures for:
   - Threads cancellation,
   - Threads degraded final read,
   - Twitter/X remote auth failure,
   - Facebook remote auth failure,
   - Threads remote auth failure.
2. Validate each fixture includes:
   - `last_error_code`,
   - `last_error_class`,
   - safe runtime metadata,
   - no raw secrets.
3. Update queue ops docs with:
   - readiness commands for Twitter/X, Facebook, and Threads,
   - new or preserved error codes,
   - targeted smoke commands,
   - explicit note that Twitter/Facebook still do not have new claimed-job lanes.
4. Update the architecture checklist with current review points for Twitter/X, Facebook, and Threads.
5. Document batch-upsert status as deferred unless equivalence tests are added and pass.

Final targeted validation:

```bash
pytest tests/scripts/test_verify_modal_readiness.py \
  tests/repositories/test_social_control_plane_imports.py \
  tests/repositories/test_social_queue_status.py
```

Threads lane validation:

```bash
pytest tests/socials/threads/posts_scrapling/test_fetcher.py \
  tests/socials/threads/posts_scrapling/test_job_runner.py \
  tests/socials/threads/posts_scrapling/test_persistence.py \
  tests/socials/threads/posts_scrapling/test_proxy.py \
  tests/repositories/test_threads_posts_scrapling_lane.py \
  tests/scripts/test_social_worker.py
```

Twitter/X validation:

```bash
pytest tests/socials/twitter/test_posts_catalog.py \
  tests/socials/test_twitter_query_building.py \
  tests/socials/test_twitter_rate_limiting.py \
  tests/socials/test_twitter_runtime_metadata.py \
  tests/scripts/test_twitter_scrape_cli.py \
  tests/scripts/test_twitter_scrape_persist.py \
  tests/api/routers/test_socials_twitter_admin_routes.py \
  tests/api/routers/test_twitter_persist_endpoint.py
```

Facebook validation:

```bash
pytest tests/socials/facebook/test_posts_catalog.py \
  tests/socials/test_facebook_document_fetch.py \
  tests/socials/test_facebook_engagement.py \
  tests/api/routers/test_socials_facebook.py \
  tests/repositories/test_social_mirror_repairs.py
```

Threads catalog validation:

```bash
pytest tests/socials/threads/test_posts_catalog.py \
  tests/socials/test_threads_scraper.py
```

Optional smoke checks after tests:

```bash
python scripts/modal/verify_modal_readiness.py --probe-remote-auth twitter --json
python scripts/modal/verify_modal_readiness.py --probe-remote-auth facebook --json
python scripts/modal/verify_modal_readiness.py --probe-remote-auth threads --json
python -m scripts.socials.twitter.scrape --query 'from:BravoTV' --start 2026-04-01 --end 2026-05-05 --max-pages 1
python scripts/socials/run_rhoslc_threads_full_refresh.py --help
```

## Orchestration Plan

Use `orchestrate-subagents`.

Main session responsibilities:

- inspect dirty worktree before workers edit,
- own `trr_backend/socials/social_season_analytics_impl.py` wrapper integration,
- own `scripts/modal/verify_modal_readiness.py`,
- own import-cycle tests,
- own docs/fixtures,
- merge worker outputs,
- run final validation.

Worker A: Threads claimed-job lane

- Write scope:
  - `trr_backend/socials/threads/posts_scrapling/job_runner.py`
  - `trr_backend/socials/threads/posts_scrapling/persistence.py`
  - `trr_backend/socials/threads/posts_scrapling/fetcher.py` only if needed for metadata plumbing
  - `tests/socials/threads/posts_scrapling/*`
  - `tests/repositories/test_threads_posts_scrapling_lane.py`
- Do not edit:
  - `trr_backend/socials/social_season_analytics_impl.py`
  - `trr_backend/socials/threads/posts_catalog/`

Worker B: Twitter/X posts catalog

- Write scope:
  - `trr_backend/socials/twitter/posts_catalog/`
  - `tests/socials/twitter/test_posts_catalog.py`
  - existing Twitter/X tests only when needed for fixture alignment
- Do not edit:
  - `social_season_analytics_impl.py` wrapper wiring
  - worker dispatch, routes, stages, or queue lanes

Worker C: Facebook posts catalog

- Write scope:
  - `trr_backend/socials/facebook/posts_catalog/`
  - `tests/socials/facebook/test_posts_catalog.py`
  - existing Facebook tests only when needed for fixture alignment
- Do not edit:
  - `social_season_analytics_impl.py` wrapper wiring
  - comments, share-detail, media mirror, routes, stages, or queue lanes

Worker D: Threads shared catalog

- Write scope:
  - `trr_backend/socials/threads/posts_catalog/`
  - `tests/socials/threads/test_posts_catalog.py`
  - existing Threads scraper tests only when needed for fixture alignment
- Do not edit:
  - `threads/posts_scrapling/`
  - worker lifecycle code

Coordination rules:

- Workers are not alone in the codebase.
- Workers must not revert edits made by other workers or unrelated dirty changes.
- Workers must list changed file paths in their final reports.
- Main session performs final wrapper integration after workers finish platform-local Modules.

## Acceptance Criteria

- Threads posts Scrapling jobs can be cancelled cleanly.
- Threads posts Scrapling jobs return a degraded summary when final DB reads are unavailable.
- Threads run finalization handles DB saturation without masking the primary result.
- Threads terminal metadata includes actionable progress, runtime, persistence, skip-reason, and error fields without raw secrets.
- `probe_remote_auth_health()` supports Twitter/X, Facebook, and Threads with safe structure flags.
- `scripts/modal/verify_modal_readiness.py` accepts `--probe-remote-auth twitter`, `facebook`, and `threads`.
- Twitter/X shared catalog behavior is owned by `trr_backend/socials/twitter/posts_catalog/`, with `_scrape_shared_twitter_posts()` preserved as a compatibility wrapper.
- Facebook shared catalog behavior is owned by `trr_backend/socials/facebook/posts_catalog/`, with `_scrape_shared_facebook_posts()` preserved as a compatibility wrapper.
- Threads shared catalog behavior is owned by `trr_backend/socials/threads/posts_catalog/`, with `threads/posts_scrapling` kept separate.
- No new comments lane, DB schema, route payload, scrape stage, app contract, or Twitter/Facebook worker lane is introduced.
- Focused validation commands pass or failures are documented with unrelated-failure evidence.

## Archive Plan

After implementation is complete and verification evidence is captured, archive this plan package with the implementation artifacts. Do not leave this revised plan as the active execution source after it has been fully implemented.

## Cleanup Note

After implementation is complete, remove or archive temporary plan-grader artifacts that are not needed for repo history, while preserving the final implementation evidence and any user-facing docs updates.
