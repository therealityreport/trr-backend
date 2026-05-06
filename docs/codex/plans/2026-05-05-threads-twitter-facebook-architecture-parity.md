# Threads, Twitter/X, and Facebook Social Architecture Parity Plan

Date: 2026-05-05
Scope: `/Users/thomashulihan/Projects/TRR/TRR-Backend`

## Summary

Improve the Threads, Twitter/X, and Facebook social scraper architecture by carrying forward the proven Instagram, TikTok, and YouTube hardening patterns now present in the repo: platform-owned Modules, small compatibility wrappers, explicit remote auth readiness, safe runtime metadata, cancellation, degraded DB handling, progress counters, and fixture-backed operator diagnostics.

Target outcome: Threads, Twitter/X, and Facebook catalog/backfill runs should be easier to operate and debug without changing DB schema, route payloads, scrape stages, or comments contracts. The work should deepen platform Modules so callers know less about platform internals while tests can verify behavior through stable Interfaces.

## Project Context

Current repo evidence:

- `docs/architecture/social-platform-module-checklist.md` defines the expected platform Module shape: constants, auth/session, proxy, fetcher, persistence, job handlers, lifecycle, scripts, and tests stay platform-local where possible.
- `trr_backend/socials/threads/posts_scrapling/` already has a posts lane (`session.py`, `proxy.py`, `fetcher.py`, `persistence.py`, `job_runner.py`) and `trr_backend/socials/threads/jobs.py` registers `threads_posts_scrapling`.
- `run_threads_posts_scrapling_job()` uses lifecycle helpers, but it has no cancellation helper and calls `lifecycle.finalize_run_status(run_id)` plus final `pg.fetch_one(...)` without degraded DB handling.
- `ThreadsPostsScraplingFetcher` returns runtime metadata and fallback state, but the job runner only partly uses persistence skip counters and does not yet match the TikTok/Instagram terminal metadata shape.
- `REMOTE_AUTH_REQUIRED_PLATFORMS` includes `twitter`, `facebook`, and `threads`, but `probe_remote_auth_health()` currently supports only `instagram` and `tiktok`.
- Cookie loaders and validators already exist for Twitter/X, Facebook, and Threads: `_load_twitter_auth_from_sources()`, `_validate_twitter_cookie_health()`, `_load_facebook_cookies_from_sources()`, `_validate_facebook_cookie_health()`, `_load_threads_cookies_from_sources()`, and `_validate_threads_cookie_health()`.
- `_scrape_shared_twitter_posts()`, `_scrape_shared_facebook_posts()`, and `_scrape_shared_threads_posts()` still own shared catalog orchestration inside `trr_backend/socials/social_season_analytics_impl.py`.
- `trr_backend/socials/twitter/` has scraper, auth, GraphQL, fallback, Crawlee, and cookie refresh Modules, but no posts catalog Module or job-handler lane.
- `trr_backend/socials/facebook/` has scraper, document fetch, Crawlee, and cookie refresh Modules, but no posts catalog Module or job-handler lane.
- The worktree is already dirty across social-control-plane, Instagram, TikTok, YouTube, Threads, Modal readiness, docs, and tests. Implementation must preserve unrelated changes.

## Assumptions

- Do not add new DB tables, migrations, route payloads, scrape stages, worker lanes, or app-facing contract changes in this pass.
- Treat Twitter/X and Facebook work as shared catalog Module extraction and readiness hardening, not a new claimed-job lane, unless a current route/job already requires one.
- Threads already has a posts lane, so it can receive job-runner hardening directly.
- Comments behavior for Twitter/X, Facebook, and Threads stays in existing surfaces unless a posts-catalog test proves posts correctness depends on it.
- Batch upsert remains deferred unless payload, conflict target, optional-column, assignment, `job_id`, and return-shape equivalence are proven.

## Deepening Opportunities

1. Threads posts lane parity

- Files: `trr_backend/socials/threads/posts_scrapling/job_runner.py`, `fetcher.py`, `persistence.py`, `session.py`, `trr_backend/socials/threads/jobs.py`, `tests/socials/threads/posts_scrapling/*`, `tests/repositories/test_threads_posts_scrapling_lane.py`.
- Problem: the Threads posts lane is a real Module, but its Interface is still shallower than Instagram/TikTok. Callers and tests still need to reason about cancellation gaps, unguarded finalization, sparse persistence skip details, and partial terminal metadata.
- Solution: deepen the Threads posts lane so `run_threads_posts_scrapling_job()` becomes the single Adapter for session, proxy, fetch, persist, progress, cancellation, retry, and final summary behavior.
- Benefits: locality improves because Threads run failures and skip reasons stay in one Module; leverage improves because the social worker can call one Interface and get consistent cancellation, retry, metadata, and degraded DB behavior.

2. Twitter/X posts catalog Module

- Files: `trr_backend/socials/twitter/scraper.py`, `auth.py`, `graphql.py`, `fallbacks.py`, new `trr_backend/socials/twitter/posts_catalog/`, `trr_backend/socials/social_season_analytics_impl.py`, `tests/socials/test_twitter_runtime_metadata.py`, `tests/socials/test_twitter_rate_limiting.py`, `tests/repositories/test_social_season_analytics.py`.
- Problem: Twitter/X has several useful Modules, but shared account catalog orchestration still lives in `_scrape_shared_twitter_posts()`. The Interface exposes too many details: cookie auth, bearer/twikit auth, full-history window policy, page-limit policy, reply filtering, profile snapshot building, shared catalog persistence, and progress counters.
- Solution: add a Twitter/X posts catalog Module that owns only shared catalog discovery and persistence orchestration. Keep `_scrape_shared_twitter_posts()` as a compatibility wrapper that injects existing helpers.
- Benefits: locality improves because catalog bugs stop requiring monolith edits; leverage improves because tests can exercise a small posts catalog Interface with fake scraper/auth Adapters instead of patching the monolith.

3. Facebook posts catalog Module

- Files: `trr_backend/socials/facebook/scraper.py`, `document_fetch.py`, `crawlee_adapter.py`, new `trr_backend/socials/facebook/posts_catalog/`, `trr_backend/socials/social_season_analytics_impl.py`, `tests/socials/test_facebook_engagement.py`, `tests/socials/test_facebook_document_fetch.py`, `tests/api/routers/test_socials_facebook.py`.
- Problem: Facebook scraping has a large implementation with SSR parsing, authenticated document fetch fallback, feed/reel/photo surfaces, comments, engagement parsing, media mirror setup, and shared catalog persistence split between scraper internals and monolith helpers.
- Solution: add a Facebook posts catalog Module that owns shared catalog orchestration and metadata assembly while leaving comment fetching, engagement parsing, and document-fetch implementation behind existing scraper/document Modules.
- Benefits: locality improves because Facebook catalog runtime metadata and progress live near platform behavior; leverage improves because callers do not need to know whether posts came from public SSR or authenticated document fetch.

4. Threads shared catalog Module

- Files: `trr_backend/socials/threads/scraper.py`, `posts_scrapling/*`, new `trr_backend/socials/threads/posts_catalog/`, `trr_backend/socials/social_season_analytics_impl.py`, `tests/socials/test_threads_scraper.py`, `tests/repositories/test_social_season_analytics.py`.
- Problem: Threads has both a posts Scrapling lane and monolith shared catalog orchestration. The two Interfaces overlap but do not share a catalog owner, so fixes to profile snapshots, retryability, and runtime metadata can diverge.
- Solution: add a Threads posts catalog Module for shared-account catalog behavior and keep `posts_scrapling` as the claimed-job lane. The new Module should reuse existing scraper/fetcher/persistence rules where practical without merging the lanes.
- Benefits: locality improves because Threads catalog behavior stops being split between monolith and lane code; leverage improves because tests can prove shared catalog metadata separately from worker job lifecycle.

5. Remote auth readiness for Twitter/X, Facebook, and Threads

- Files: `trr_backend/socials/social_season_analytics_impl.py`, `trr_backend/socials/control_plane/worker_health.py`, `scripts/modal/verify_modal_readiness.py`, `tests/scripts/test_verify_modal_readiness.py`, `tests/repositories/test_social_season_analytics.py`.
- Problem: worker auth capabilities report Twitter/X, Facebook, and Threads auth state, and all three platforms are listed as remote-auth-required, but the remote probe Interface rejects them. This is the same class of operator-facing bug just fixed for TikTok.
- Solution: extend `probe_remote_auth_health()` and readiness CLI choices for `twitter`, `facebook`, and `threads`, returning safe structure fields only.
- Benefits: locality improves because readiness logic uses the existing cookie handler registry; leverage improves because Modal readiness can validate all remote-auth-required platforms through one Interface.

## Implementation Changes

### Phase 0: Current-State Tests First

Add or update tests before behavior changes where feasible.

1. Threads job-runner parity tests.
   - File: `tests/socials/threads/posts_scrapling/test_job_runner.py`.
   - Add cancellation tests for job-cancelled and run-cancelled states.
   - Add degraded-final-read and finalize-saturation tests.
   - Add terminal metadata assertions for `persist_counters`, `posts_skipped`, `posts_skipped_by_reason`, `fetcher_runtime`, `runtime_metadata`, `stop_reason`, and safe cookie/proxy diagnostics.
   - Expected pre-fix result: fails because the runner has no cancellation helper and does not catch finalization/final read saturation.

2. Remote auth probe tests.
   - Files: `tests/repositories/test_social_season_analytics.py` and `tests/scripts/test_verify_modal_readiness.py`.
   - Add safe-structure tests for `twitter`, `facebook`, and `threads`.
   - Add CLI parsing/readiness tests for `--probe-remote-auth twitter`, `facebook`, and `threads`.
   - Expected pre-fix result: unsupported platform or rejected CLI choices.

3. Catalog extraction tests.
   - Files: `tests/repositories/test_social_season_analytics.py`, plus platform-local tests under `tests/socials/twitter/`, `tests/socials/facebook/`, and `tests/socials/threads/` if a colocated path exists or is added.
   - Add wrapper delegation tests proving `_scrape_shared_twitter_posts()`, `_scrape_shared_facebook_posts()`, and `_scrape_shared_threads_posts()` delegate to platform posts catalog Modules.
   - Add fixture-backed no-network tests for one normal catalog row and one retryable/empty/error metadata case per platform where existing fixtures allow it.

### Phase 1: Harden Threads Posts Lane

1. Add cancellation support in `trr_backend/socials/threads/posts_scrapling/job_runner.py`.
   - Add a lane-local cancellation exception.
   - Add `_raise_if_cancelled(job_id, run_id, ...)` modeled on Instagram/TikTok posts.
   - Check after session resolution, warmup, fetch, persist, and before final completion.
   - Finish cancelled jobs with `last_error_code="threads_posts_scrapling_cancelled"` plus `cancel_scope`, `job_status`, `run_status`, and safe runtime metadata.

2. Add degraded DB handling.
   - Catch `pg.DatabaseServiceUnavailableError` around `lifecycle.finalize_run_status(run_id)`.
   - Catch the final `pg.fetch_one(...)` summary read and return a degraded summary with stable fields.

3. Improve terminal metadata.
   - Extend `PersistedThreadsPosts` or returned diagnostics to include `posts_skipped_by_reason`.
   - Include `listing_progress`, `stage_counters`, `persist_counters`, `fetch_counters`, `fetcher_runtime`, `runtime_metadata`, `source_runtime`, `stop_reason`, and `activity` on success/failure.
   - Preserve safe fingerprints only; never include raw cookie values.

4. Keep fallback taxonomy explicit.
   - Preserve legacy `ThreadsScraper` fallback only when `ThreadsPostsScraplingFetcher` fails with no posts and the current retry policy says fallback is allowed.
   - Add tests proving auth/warmup failures do not become noisy fallback chains without evidence.

### Phase 2: Extend Remote Auth Readiness

1. Extend `probe_remote_auth_health(platform)`.
   - Keep Instagram and TikTok behavior unchanged.
   - For Twitter/X, use `_load_twitter_auth_from_sources()` plus `_validate_twitter_cookie_health()` and include safe flags: `has_auth_token`, `has_ct0`, `has_bearer_token`, `has_twikit_credentials`.
   - For Facebook, use `_load_facebook_cookies_from_sources()` plus `_validate_facebook_cookie_health()` and include `has_c_user`, `has_xs`.
   - For Threads, use `_load_threads_cookies_from_sources()` plus `_validate_threads_cookie_health()` and include `has_sessionid`, `has_csrftoken`.

2. Update readiness CLI and Modal checks.
   - Allow `--probe-remote-auth twitter`, `facebook`, and `threads`.
   - Keep unsupported platform behavior explicit.
   - Add tests proving probe failures become blocking probe failures when strict readiness checks ask for them.

### Phase 3: Extract Twitter/X Posts Catalog Module

1. Create `trr_backend/socials/twitter/posts_catalog/`.
   - Add `__init__.py`.
   - Add `catalog.py` with `scrape_shared_twitter_posts(...)`.
   - Use dependency injection for monolith helpers and persistence to avoid circular imports.

2. Move shared catalog orchestration out of `_scrape_shared_twitter_posts()`.
   - Preserve full-history window behavior, page-limit behavior, reply filtering, profile snapshot, `posts_checked`, `pages_scanned`, `persist_counters`, and `last_retrieval_meta`.
   - Keep `_scrape_shared_twitter_posts()` as a compatibility wrapper returning `(rows, retrieval_meta)`.

3. Keep Twitter/X comments and quotes out of scope.
   - Do not move `fetch_tweet_replies()`, quote fetching, media mirroring, or repair scripts unless posts catalog tests prove they are coupled.

### Phase 4: Extract Facebook Posts Catalog Module

1. Create `trr_backend/socials/facebook/posts_catalog/`.
   - Add `__init__.py`.
   - Add `catalog.py` with `scrape_shared_facebook_posts(...)`.
   - Keep scraper/document fetch details inside existing Facebook Modules.

2. Move shared catalog orchestration out of `_scrape_shared_facebook_posts()`.
   - Preserve `FacebookScrapeConfig` defaults, delay env behavior, max scroll env behavior, profile snapshot, `last_retrieval_meta`, and shared catalog progress.
   - Keep `_scrape_shared_facebook_posts()` as a compatibility wrapper.

3. Keep Facebook comments, share details, and media mirror follow-up out of scope.
   - Do not move `_upsert_facebook_comment_tree()` or detail refresh behavior in this pass.

### Phase 5: Extract Threads Shared Catalog Module

1. Create `trr_backend/socials/threads/posts_catalog/`.
   - Add `__init__.py`.
   - Add `catalog.py` with `scrape_shared_threads_posts(...)`.
   - Keep claimed-job `posts_scrapling` lane separate.

2. Move shared catalog orchestration out of `_scrape_shared_threads_posts()`.
   - Preserve `ThreadsScrapeConfig`, env delay behavior, profile snapshot, `last_retrieval_meta`, shared catalog progress, and existing persistence callbacks.
   - Keep `_scrape_shared_threads_posts()` as a compatibility wrapper.

3. Add import-cycle coverage.
   - Extend `tests/repositories/test_social_control_plane_imports.py` so the new `twitter.posts_catalog`, `facebook.posts_catalog`, and `threads.posts_catalog` Modules import before the legacy compatibility module.

### Phase 6: Operator Fixtures, Docs, And Batch-Upsert Decision

1. Add metadata fixtures.
   - Extend `tests/fixtures/socials/run_metadata/` with representative Threads cancellation/degraded metadata and Twitter/Facebook/Threads remote-auth failure metadata.
   - Validate that fixtures preserve `last_error_code`, `last_error_class`, safe runtime metadata, and no raw secrets.

2. Update operations docs.
   - Extend `docs/runbooks/social_worker_queue_ops.md` with new Threads/Twitter/Facebook readiness commands, error codes, and smoke commands.
   - Extend `docs/architecture/social-platform-module-checklist.md` with current review points for Twitter/X, Facebook, and Threads.

3. Batch upsert remains gated.
   - Document implemented/deferred status for Twitter/X, Facebook, and Threads.
   - Do not switch to `_pg_upsert_many` unless equivalence tests prove row shape and optional-column behavior.

## Validation

Run Threads lane tests:

```bash
pytest tests/socials/threads/posts_scrapling/test_fetcher.py \
  tests/socials/threads/posts_scrapling/test_job_runner.py \
  tests/socials/threads/posts_scrapling/test_persistence.py \
  tests/socials/threads/posts_scrapling/test_proxy.py \
  tests/repositories/test_threads_posts_scrapling_lane.py \
  tests/scripts/test_social_worker.py
```

Expected result: Threads posts Scrapling tests pass, including new cancellation, degraded DB, metadata, and safe-runtime tests.

Run Twitter/X tests:

```bash
pytest tests/socials/test_twitter_query_building.py \
  tests/socials/test_twitter_rate_limiting.py \
  tests/socials/test_twitter_runtime_metadata.py \
  tests/scripts/test_twitter_scrape_cli.py \
  tests/scripts/test_twitter_scrape_persist.py \
  tests/api/routers/test_socials_twitter_admin_routes.py \
  tests/api/routers/test_twitter_persist_endpoint.py
```

Expected result: Twitter/X scraper, CLI, persistence, and admin route tests pass, including new posts catalog wrapper/delegation tests.

Run Facebook tests:

```bash
pytest tests/socials/test_facebook_document_fetch.py \
  tests/socials/test_facebook_engagement.py \
  tests/api/routers/test_socials_facebook.py \
  tests/repositories/test_social_mirror_repairs.py
```

Expected result: Facebook document fetch, engagement parsing, route, mirror, and catalog tests pass.

Run shared integration tests:

```bash
pytest tests/scripts/test_verify_modal_readiness.py \
  tests/repositories/test_social_control_plane_imports.py \
  tests/repositories/test_social_queue_status.py
```

Expected result: readiness probing, import compatibility, and queue/admin error-code surfaces remain stable.

If `tests/repositories/test_social_season_analytics.py` is too broad, run targeted node IDs covering:

- `test_probe_remote_auth_health_reports_*`
- `_scrape_shared_twitter_posts`
- `_scrape_shared_facebook_posts`
- `_scrape_shared_threads_posts`
- `_upsert_facebook_post`
- `_upsert_meta_threads_post`
- shared catalog persistence progress

Optional smoke checks after tests:

```bash
python scripts/modal/verify_modal_readiness.py --probe-remote-auth twitter --json
python scripts/modal/verify_modal_readiness.py --probe-remote-auth facebook --json
python scripts/modal/verify_modal_readiness.py --probe-remote-auth threads --json
python -m scripts.socials.twitter.scrape --query 'from:BravoTV' --start 2026-04-01 --end 2026-05-05 --max-pages 1
python scripts/socials/run_rhoslc_threads_full_refresh.py --help
```

Expected result: readiness probes return structured payloads, Twitter/X smoke returns bounded diagnostics, and Threads helper script remains importable.

## Acceptance Criteria

- Threads posts Scrapling jobs can be cancelled cleanly and return a degraded summary when final DB reads are unavailable.
- Threads terminal metadata includes actionable progress, runtime, persistence, skip-reason, and error fields without leaking secrets.
- `probe_remote_auth_health()` supports Twitter/X, Facebook, and Threads with safe structure flags.
- Modal readiness accepts `--probe-remote-auth twitter`, `facebook`, and `threads`.
- Twitter/X shared catalog behavior is owned by `trr_backend/socials/twitter/posts_catalog/`, with a compatibility wrapper in the monolith.
- Facebook shared catalog behavior is owned by `trr_backend/socials/facebook/posts_catalog/`, with a compatibility wrapper in the monolith.
- Threads shared catalog behavior is owned by `trr_backend/socials/threads/posts_catalog/`, with the existing `posts_scrapling` lane kept separate.
- No new comments lane, DB schema, route payload, scrape stage, or worker lane is introduced.
- Focused validation commands pass or failures are documented with unrelated-failure evidence.

## Risks / Open Questions

- Twitter/X has multiple retrieval paths (`graphql`, `twikit`, `syndication`, `playwright`). The posts catalog Module should not pretend these are interchangeable Adapters until tests prove at least two real Adapters behind the same Interface.
- Facebook extraction can become shallow if it simply forwards to `FacebookScraper.scrape()` and re-exports all monolith helper facts. Apply the deletion test: callers should know less after extraction.
- Threads has both shared catalog and claimed-job lanes. Do not merge their Interfaces; shared catalog orchestration and worker lifecycle are different seams.
- Batch upsert may not be safe for any of the three platforms because current upsert helpers own optional-column gates and assignment payload behavior.
- The dirty worktree overlaps target files. Executors must inspect diffs before editing and avoid reverting unrelated user changes.

## Recommended Handoff

Use `orchestrate-subagents` after Phase 0 tests are defined.

Suggested ownership scopes:

1. Worker A: Threads posts lane hardening and Threads lane tests.
2. Worker B: Twitter/X posts catalog Module and Twitter/X focused tests.
3. Worker C: Facebook posts catalog Module and Facebook focused tests.
4. Worker D or main session: Threads shared catalog Module, remote auth readiness, docs/fixtures, and final shared validation.

Main session should own integration in `trr_backend/socials/social_season_analytics_impl.py`, readiness CLI changes, import-cycle tests, and final validation to avoid overlapping wrapper edits.

## Ready For Execution

Ready for execution after the executor re-checks the dirty worktree and confirms disjoint ownership of the touched files. Start with Phase 0 tests and Phase 2 remote auth because those are concrete operator-facing bugs.
