# Revised Plan: TikTok and YouTube Social Architecture Parity

Date: 2026-05-05
Source plan: `docs/codex/plans/2026-05-05-tiktok-youtube-architecture-parity.md`
Scope: `/Users/thomashulihan/Projects/TRR/TRR-Backend`

## Summary

Apply the proven Instagram social-scraper hardening patterns to TikTok and YouTube without creating new DB schema, route payloads, scrape stages, or worker lanes. The implementation should improve operator-facing truth for TikTok posts and YouTube catalog runs: cancellation, retryability, auth readiness, progress, and failure metadata should match what the current backend can actually prove.

Success is measurable when:

- TikTok `posts_scrapling` cancellation and degraded DB tests pass.
- TikTok remote auth probing returns a structured TikTok readiness payload instead of an unsupported-platform error.
- YouTube shared catalog scraping delegates out of `social_season_analytics_impl.py` into a YouTube-owned posts catalog Module while preserving the existing `_scrape_shared_youtube_posts` compatibility wrapper.
- The focused TikTok, YouTube, and shared social regression tests pass.
- Within 30 days of use, recent TikTok/YouTube social run failures should have actionable `last_error_code`, `error_class`, progress, and runtime metadata instead of generic failed-state summaries.

## Current Repo Evidence

- `docs/architecture/social-platform-module-checklist.md` says constants, auth/session, proxy, fetcher, persistence, job handlers, lifecycle, scripts, and tests should live in platform Modules where possible. It also blocks TikTok comments ingestion until the comments contract is documented.
- `trr_backend/socials/tiktok/posts_scrapling/job_runner.py` already orchestrates session, proxy, fetcher, warmup, secUid resolution, pagination, persistence, progress, and finish, but it has no cancellation helper and directly finalizes/reads the final row without degraded DB handling.
- `trr_backend/socials/tiktok/posts_scrapling/fetcher.py` hardcodes `TIKTOK_POST_PAGE_SIZE = 30`.
- `probe_remote_auth_health()` in `trr_backend/socials/social_season_analytics_impl.py` only supports Instagram, while worker auth capabilities already report TikTok authentication.
- `trr_backend/modal_jobs.py` exposes `probe_social_remote_auth(platform)` and delegates to `probe_remote_auth_health(platform)`, so adding TikTok support there fixes both local and Modal readiness checks.
- `_scrape_shared_youtube_posts()` currently owns YouTube shared catalog orchestration inside `trr_backend/socials/social_season_analytics_impl.py`.
- `trr_backend/socials/youtube/` currently contains `api_client.py`, `media_resolver.py`, `crawlee_adapter.py`, and `scraper.py`; there is no posts catalog Module.

## Non-Goals

- Do not add or wire a TikTok comments ingestion lane.
- Do not add a YouTube queue stage, worker lane, route payload, or DB table.
- Do not change schema or migrations in this pass.
- Do not force batch upsert for TikTok or YouTube unless return shape, conflict target, optional columns, and assignment behavior are proven equivalent to current per-row helpers.
- Do not remove compatibility wrappers from `trr_backend/repositories/social_season_analytics.py` or `social_season_analytics_impl.py` until import-ownership tests prove downstream callers are updated.

## Phase 0: Current-State Tests First

Add or update targeted tests before implementation. These should fail against the current code, then pass after the changes.

1. TikTok cancellation and degraded DB tests.
   - File: `tests/socials/tiktok/posts_scrapling/test_job_runner.py`
   - Add a job-cancelled test that simulates `social.scrape_jobs.status = 'cancelled'` and expects `last_error_code == 'tiktok_posts_scrapling_cancelled'`.
   - Add a run-cancelled test that simulates `social.scrape_runs.status = 'cancelled'` and expects cancel-scope metadata.
   - Add a degraded-final-read test that raises `pg.DatabaseServiceUnavailableError` from final `pg.fetch_one` and expects a returned degraded summary.
   - Expected pre-fix result: these fail because TikTok job runner has no cancellation helper and does not catch final read/finalize saturation.

2. TikTok page-size env tests.
   - File: `tests/socials/tiktok/posts_scrapling/test_fetcher.py`
   - Add tests for default, invalid env, lower bound, upper bound, and valid env value for `SOCIAL_TIKTOK_POSTS_SCRAPLING_PAGE_SIZE`.
   - Expected pre-fix result: these fail because page size is a constant.

3. TikTok remote auth probe tests.
   - Files: `tests/scripts/test_verify_modal_readiness.py` and the closest existing control-plane auth/probe test.
   - Add a test that `probe_remote_auth_health("tiktok")` returns `platform`, `ready`, `reason`, and safe cookie-structure fields.
   - Add a readiness-script test that `--probe-remote-auth tiktok` is accepted.
   - Expected pre-fix result: these fail with unsupported platform or rejected CLI choices.

4. YouTube catalog extraction tests.
   - File: `tests/repositories/test_youtube_catalog_backfill_diagnostics.py`
   - Preserve the existing `youtube_empty_channel_page` test.
   - Add a test that `_scrape_shared_youtube_posts()` delegates to `trr_backend.socials.youtube.posts_catalog.catalog.scrape_shared_youtube_posts`.
   - Expected pre-fix result: delegation import or call assertion fails because the module does not exist.

## Phase 1: Harden TikTok Posts Lane

1. Add cancellation support in `trr_backend/socials/tiktok/posts_scrapling/job_runner.py`.
   - Add `TikTokPostsScraplingCancelledError`.
   - Add `_raise_if_cancelled(job_id, run_id, runtime_metadata=None, conn=None)` modeled on Instagram posts.
   - Check cancellation after warmup, after secUid resolution, before each page fetch, after each persistence call, and before final completion.
   - On cancellation, finish the job with `status="cancelled"` if the row/run is already cancelled or `status="failed"` only if the lifecycle API cannot represent cancellation for this lane.
   - Include `last_error_code="tiktok_posts_scrapling_cancelled"`, `cancel_scope`, `job_status`, `run_status`, and safe runtime metadata.

2. Add degraded DB handling.
   - Wrap `lifecycle.finalize_run_status(run_id)` in `try/except pg.DatabaseServiceUnavailableError`.
   - Wrap the final `pg.fetch_one` job summary read in the same degraded-summary pattern as Instagram posts.
   - The degraded summary must include `id`, `run_id`, `platform`, `job_type`, `status`, `items_found`, `error_message`, and metadata containing `degraded_summary` and `database_service_unavailable`.

3. Improve TikTok terminal metadata.
   - Track `posts_skipped` and `posts_skipped_by_reason` from `persist_tiktok_posts`.
   - Extend `PersistedTikTokPosts` or return a compatible diagnostics structure from persistence.
   - On success and failure, include `listing_progress`, `stage_counters`, `persist_counters`, `fetcher_runtime`, `runtime_metadata`, `stop_reason`, and `activity`.
   - Keep `last_error_code` and `last_error_class` on failed or retrying jobs.

4. Make page size lane-local and bounded.
   - Replace the default argument `count: int = TIKTOK_POST_PAGE_SIZE` with a helper-backed default.
   - Add `tiktok_posts_scrapling_page_size()` in `fetcher.py`.
   - Read `SOCIAL_TIKTOK_POSTS_SCRAPLING_PAGE_SIZE`.
   - Default to `30`.
   - Bound to a safe range. Use `10..50` unless current TikTok API evidence requires a tighter range.
   - Keep explicit `count=` overrides working for tests.

5. Preserve direct API failure taxonomy.
   - Do not add new broad fallback behavior.
   - Confirm `challenge_or_blocked`, `non_json_response`, `http_401`, and `http_403` stay terminal for the direct TikTok posts path unless a later plan explicitly enables another adapter.
   - Keep current yt-dlp-to-API fallback only for `ytdlp_unavailable`, `ytdlp_timeout`, and `ytdlp_nonzero_exit`.

6. Add TikTok remote auth support.
   - Update `probe_remote_auth_health(platform)` to branch on `instagram` and `tiktok`.
   - For TikTok, use `_load_tiktok_cookies_from_sources()` plus `_validate_tiktok_cookie_health()`.
   - Return safe fields only, such as `has_sessionid`, `has_sid_tt`, `has_ms_token`, `ready`, and `reason`.
   - Update `scripts/modal/verify_modal_readiness.py` to allow `--probe-remote-auth tiktok`.

## Phase 2: Extract YouTube Posts Catalog Module

1. Create `trr_backend/socials/youtube/posts_catalog/`.
   - Add `__init__.py`.
   - Add `catalog.py` as the posts catalog orchestration owner.
   - Add `types.py` only if a small dataclass improves the interface.
   - Add `persistence.py` only when the persistence adapter can be tested without creating circular imports.

2. Move posts-only shared catalog orchestration out of the monolith.
   - Move the body of `_scrape_shared_youtube_posts()` into `posts_catalog/catalog.py` as `scrape_shared_youtube_posts(...)`.
   - Keep `_scrape_shared_youtube_posts()` in `social_season_analytics_impl.py` as a compatibility wrapper.
   - The wrapper should delegate and preserve the same return shape: `(rows, retrieval_meta)`.
   - Preserve `youtube_empty_channel_page`, `ytdlp_available`, profile snapshot, canonical handle, and `persist_counters`.

3. Keep YouTube comments, transcripts, and media resolver out of scope.
   - Do not move `fetch_comments()`, transcript fetching, or `resolve_youtube_media()` during this pass.
   - Only touch them if a posts catalog test proves the posts path is coupled to the behavior.

4. Avoid inventing a YouTube worker lane.
   - Do not add `trr_backend/socials/youtube/jobs.py` or register a YouTube platform job handler unless a current route/job already requires it and the change is re-planned.
   - The new Module is for shared catalog orchestration, not a new stage.

5. Persistence adapter rule.
   - First pass may accept callbacks for `_upsert_shared_catalog_post` and `_upsert_youtube_video` from the compatibility wrapper to avoid a broad helper move.
   - If extracting payload building is safe, move only the YouTube payload builder into `posts_catalog/persistence.py` and keep `_upsert_youtube_video()` delegating.
   - Preserve optional-column behavior for `hashtags`, `mentions`, `is_short`, `source_surface`, transcript fields, and `user_avatar_url`.

## Phase 3: Shared Catalog Progress and Optional Batch Upsert

1. Remove duplicate progress logic where safe.
   - Prefer an existing shared catalog progress owner, such as `trr_backend/socials/account_catalog/catalog_progress.py`, if it already fits.
   - Otherwise add a small helper next to the catalog progress code, not inside individual platform scrapers.
   - Keep progress payload fields stable: `phase`, `pages_scanned`, `posts_checked`, `matched_posts`, `saved_posts`, and optional `total_posts`.

2. Gate batch upsert by contract equivalence.
   - Compare TikTok and YouTube payloads against `_upsert_tiktok_post()` and `_upsert_youtube_video()`.
   - Verify conflict columns, optional columns, assignment payloads, returned row fields, and side effects.
   - If equivalent, add platform-local batch upsert tests and use `_pg_upsert_many`.
   - If not equivalent, keep the per-row path and document the blocker in `docs/architecture/social-platform-module-checklist.md`.

3. Update import ownership tests.
   - Update `tests/repositories/test_social_control_plane_imports.py` only if ownership moved.
   - Keep compatibility wrappers until all current callers are updated and tests prove the import path is stable.

## Phase 4: Validation

Run focused tests after each phase:

```bash
pytest tests/socials/tiktok/posts_scrapling/test_fetcher.py \
  tests/socials/tiktok/posts_scrapling/test_job_runner.py \
  tests/socials/tiktok/posts_scrapling/test_persistence.py \
  tests/repositories/test_tiktok_posts_scrapling_start.py \
  tests/repositories/test_tiktok_posts_scrapling_lane.py \
  tests/repositories/test_tiktok_posts_scrapling_worker_lane.py
```

Expected result: TikTok tests pass, including new cancellation, degraded DB, page-size, metadata, and auth-probe coverage.

```bash
pytest tests/socials/youtube/test_scraper.py \
  tests/socials/youtube/test_media_resolver.py \
  tests/repositories/test_youtube_catalog_backfill_diagnostics.py \
  tests/scripts/test_youtube_scrape_cli.py
```

Expected result: YouTube scraping, empty-channel diagnostics, media resolver, and CLI behavior remain stable.

```bash
pytest tests/scripts/test_verify_modal_readiness.py \
  tests/repositories/test_social_control_plane_imports.py \
  tests/repositories/test_social_queue_status.py
```

Expected result: readiness probing, import compatibility, and queue failure display remain stable.

If `tests/repositories/test_social_season_analytics.py` is needed, run targeted node IDs for:

- `_scrape_shared_tiktok_posts`
- `_scrape_shared_youtube_posts`
- `_upsert_tiktok_post`
- `_upsert_youtube_video`
- shared catalog persistence
- TikTok/Twitter/YouTube queue failure metadata

Optional smoke checks after unit tests:

```bash
python scripts/modal/verify_modal_readiness.py --probe-remote-auth tiktok --json
python -m scripts.socials.tiktok.smoke_posts_scrapling --account bravotv --max-pages 1
python -m scripts.socials.youtube.scrape --channel bravo --max-results 5 --no-comments
```

Expected result: all commands return structured output without generic unsupported-platform or missing-metadata failures.

## Stop Rules

- Stop and re-plan if implementation needs a new table, route payload, scrape stage, worker lane, run status, or metadata key.
- Stop if YouTube extraction creates circular imports that require moving broad monolith helpers.
- Stop if TikTok or YouTube batch upsert cannot preserve current row shape or optional-column behavior.
- Stop TikTok comments work unless the comments contract evidence in `docs/architecture/social-platform-module-checklist.md` is satisfied.
- Stop before changing app-facing behavior; create an app follow-through plan if an API response or admin UI contract changes.

## Recommended Handoff

Use `orchestrate-subagents` after Phase 0 tests are defined:

- Worker A owns TikTok posts lane files and TikTok-focused tests.
- Worker B owns YouTube posts catalog extraction and YouTube-focused tests.
- Main session owns compatibility wrapper integration, shared catalog progress cleanup, and final regression selection.

Workers are not alone in the codebase. They must not revert unrelated dirty worktree changes and must adapt to changes already present.

## Acceptance Criteria

- New failing tests are added before implementation and pass after implementation.
- TikTok posts jobs support cancellation and degraded DB finalization.
- TikTok posts job metadata includes actionable progress, runtime, persistence, and error fields.
- TikTok remote auth probe supports TikTok locally and through Modal readiness.
- YouTube shared catalog behavior is owned by `trr_backend/socials/youtube/posts_catalog/`, with a compatibility wrapper in the monolith.
- Existing TikTok and YouTube route/job contracts remain unchanged.
- No TikTok comments ingestion lane is introduced.
- Focused validation commands pass or failures are documented with unrelated-failure evidence.

## Archive Plan

After this plan is completely implemented and verified, archive the active plan instead of leaving it in the active planning path. Move or mark the canonical plan and this `REVISED_PLAN.md` as completed, implemented, failed, or superseded with evidence, so future planning does not treat completed or failed work as an untried plan.

## Cleanup Note

After this plan is completely implemented and verified, delete any temporary planning artifacts that are no longer needed, including generated audit, scorecard, suggestions, comparison, patch, benchmark, and validation files. Do not delete them before implementation is complete because they are part of the execution evidence trail.

## Ready For Execution

Ready for execution after the executor reviews the current dirty worktree and confirms ownership of the touched files.
