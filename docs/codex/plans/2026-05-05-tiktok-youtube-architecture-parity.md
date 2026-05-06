# TikTok and YouTube Social Architecture Parity Plan

Date: 2026-05-05
Scope: `/Users/thomashulihan/Projects/TRR/TRR-Backend`

## Summary

Improve the TikTok and YouTube social scraper architecture by carrying forward the Instagram hardening patterns that are already proven in the repo: lane-local modules, explicit failure taxonomy, safe fallback behavior, progress metadata, cancellation, degraded DB handling, and batch-oriented catalog persistence where the current interface makes that safe.

Target user-visible outcome: TikTok and YouTube catalog/backfill runs should be easier to operate, easier to debug from admin run metadata, and less likely to retry or fall back incorrectly after known terminal failures. The implementation should not create new platform tables, route payloads, or comments lanes.

## Project Context

Current instructions require backend-first work for schema, API, auth, and shared contract changes. This plan is backend-only unless implementation discovers an app-facing route or payload change.

Relevant live files:

- `trr_backend/socials/instagram/posts_scrapling/job_runner.py` contains the hardened job-runner shape: cancellation detection, rich terminal metadata, `last_error_code`, retry scheduling, deferred run-finalization handling, and degraded final job summary handling.
- `trr_backend/socials/instagram/catalog_ingest.py` owns Instagram catalog behavior, including the recent configurable GraphQL page size and batch catalog upsert path.
- `trr_backend/socials/tiktok/posts_scrapling/session.py`, `proxy.py`, `fetcher.py`, `persistence.py`, and `job_runner.py` already form a real TikTok posts lane, but the job runner is thinner than Instagram's.
- `trr_backend/socials/youtube/scraper.py` still owns channel page scraping, yt-dlp enrichment, YouTubei comments, Data API fallback metadata, retryability, and parsing in one large Module.
- `trr_backend/socials/youtube/api_client.py`, `media_resolver.py`, and `crawlee_adapter.py` are separate Modules, but there is no YouTube posts lane with session/fetcher/persistence/job-runner seams.
- `trr_backend/socials/social_season_analytics_impl.py` still owns `_scrape_shared_tiktok_posts`, `_scrape_shared_youtube_posts`, `_upsert_tiktok_post`, `_upsert_youtube_video`, and generic `_persist_shared_catalog_posts_with_progress`.
- `docs/architecture/social-platform-module-checklist.md` already defines the intended platform Module shape and explicitly blocks TikTok comments ingestion until the comments contract is documented.

## Assumptions

- Do not implement a new TikTok or YouTube comments ingestion lane in this pass.
- Keep existing DB schema and route payloads unless a current bug cannot be fixed without a separate schema/API plan.
- Keep `trr_backend/repositories/social_season_analytics.py` compatibility imports working while moving behavior behind deeper platform Modules.
- Use Instagram as the source of proven operational patterns, not as a requirement to make TikTok or YouTube identical.

## Deepening Opportunities

1. TikTok posts lane hardening

Files:

- `trr_backend/socials/tiktok/posts_scrapling/job_runner.py`
- `trr_backend/socials/tiktok/posts_scrapling/fetcher.py`
- `trr_backend/socials/tiktok/posts_scrapling/persistence.py`
- `trr_backend/socials/tiktok/scraper.py`
- `trr_backend/socials/social_season_analytics_impl.py`
- `tests/socials/tiktok/posts_scrapling/*`
- `tests/repositories/test_tiktok_posts_scrapling_*.py`

Problem:

The TikTok posts lane is a real Module, but its interface is still shallower than Instagram's. Callers get less leverage from the Module because cancellation, degraded DB behavior, page-size policy, terminal failure metadata, and persistence diagnostics are not all local to the lane. The current job runner also assumes final status reads and run finalization will succeed, while Instagram now handles DB saturation more carefully.

Solution:

Deepen the TikTok posts lane so the job runner is the single adapter between social lifecycle and TikTok posts fetching/persistence. Port the relevant Instagram behaviors, adjusted to TikTok names and error codes.

Benefits:

Locality improves because TikTok run failures, retries, and diagnostics become lane-local. Leverage improves because worker dispatch and admin progress can trust the same small job-runner interface without knowing TikTok internals.

2. YouTube posts catalog lane extraction

Files:

- `trr_backend/socials/youtube/scraper.py`
- `trr_backend/socials/youtube/api_client.py`
- `trr_backend/socials/youtube/media_resolver.py`
- new `trr_backend/socials/youtube/posts_catalog/{fetcher.py,persistence.py,job_runner.py}` or equivalent narrower name
- `trr_backend/socials/social_season_analytics_impl.py`
- `tests/socials/youtube/*`
- `tests/repositories/test_youtube_catalog_backfill_diagnostics.py`

Problem:

YouTube has useful code, but the main `YouTubeScraper` Module is shallow: the interface exposes too much implementation knowledge about channel pages, shorts, continuations, yt-dlp enrichment, comments, transcripts, first-page diagnostics, and profile snapshots. `_scrape_shared_youtube_posts` in the monolith then coordinates persistence and catalog progress directly.

Solution:

Add a deeper YouTube posts catalog Module that owns only channel posts discovery and catalog persistence. Leave comments, transcript refresh, and media resolver behavior in their existing surfaces unless the implementation has to touch them for posts correctness.

Benefits:

Locality improves because YouTube catalog bugs stop requiring a monolith edit plus a scraper edit. Leverage improves because the catalog/backfill path can call a single YouTube posts interface that returns rows plus typed retrieval metadata.

3. Shared catalog persistence parity

Files:

- `trr_backend/socials/tiktok/posts_scrapling/persistence.py`
- `trr_backend/socials/youtube/posts_catalog/persistence.py`
- `trr_backend/socials/social_season_analytics_impl.py`
- `trr_backend/socials/instagram/catalog_ingest.py`
- `tests/repositories/test_social_season_analytics.py`

Problem:

Instagram recently moved shared catalog upserts to a batch-oriented path. TikTok and YouTube still persist catalog rows through per-item callbacks in `_persist_shared_catalog_posts_with_progress` or per-item platform helpers. That keeps progress simple, but DB round trips remain tied to item count.

Solution:

First add platform-local persistence adapters that build complete payloads using the same field rules as `_upsert_tiktok_post` and `_upsert_youtube_video`. Then use `_pg_upsert_many` only where conflict rules, optional columns, and return shape can match the current per-row helper contract. If batch upsert cannot be made contract-equivalent in the first pass, keep the per-row path and document the blocker in `docs/architecture/social-platform-module-checklist.md`.

Benefits:

Locality improves because platform field mapping moves closer to the platform Module. Leverage improves because catalog runs can gain Instagram-style DB efficiency without spreading table-shape details across callers.

## Implementation Changes

Phase 1: TikTok posts lane parity

1. Add TikTok cancellation handling modeled on Instagram posts.
   - Add a lane-local cancellation exception in `trr_backend/socials/tiktok/posts_scrapling/job_runner.py`.
   - Check `social.scrape_jobs.status` and `social.scrape_runs.status` between warmup, secUid resolution, each page fetch, and each persist step.
   - On cancellation, finish the job with a TikTok-specific cancellation code such as `tiktok_posts_scrapling_cancelled`, include cancel scope, and finalize the run.

2. Harden TikTok terminal metadata and degraded DB behavior.
   - Track `posts_skipped` and `posts_skipped_by_reason` from `persist_tiktok_posts`.
   - Include `listing_progress`, `fetcher_runtime`, `runtime_metadata`, `persist_counters`, `pages_fetched`, and `stop_reason` in both success and failure metadata.
   - Wrap `finalize_run_status` and final `pg.fetch_one` in `DatabaseServiceUnavailableError` handling, returning a degraded summary like Instagram when needed.

3. Make TikTok page-size policy explicit.
   - Replace the hardcoded `TIKTOK_POST_PAGE_SIZE = 30` with a bounded helper, for example `tiktok_posts_scrapling_page_size()`.
   - Source the value from `SOCIAL_TIKTOK_POSTS_SCRAPLING_PAGE_SIZE`, defaulting to the current `30`.
   - Validate with unit tests for default, min, max, and invalid env values.

4. Normalize TikTok fallback/failure taxonomy.
   - Keep yt-dlp as the production default in `TikTokScraper`.
   - Ensure auth/challenge failures such as `challenge_or_blocked`, `non_json_response`, `http_401`, and `http_403` are terminal for the direct API path and do not cause noisy alternate fallbacks unless explicitly configured.
   - Preserve the current allowed yt-dlp-to-API fallback only for `ytdlp_unavailable`, `ytdlp_timeout`, and `ytdlp_nonzero_exit`.
   - Add tests that prove unrecoverable direct API failures are reported once with `last_error_code`.

5. Extend remote auth readiness for TikTok.
   - `REMOTE_AUTH_REQUIRED_PLATFORMS` includes TikTok, but `probe_remote_auth_health()` currently only supports Instagram.
   - Add TikTok support using existing `_load_tiktok_cookies_from_sources()` and `_validate_tiktok_cookie_health()` helpers.
   - Update `probe_social_remote_auth`/`verify_modal_readiness.py` coverage so TikTok remote auth can be checked without raising "Unsupported remote auth probe platform".

Phase 2: YouTube posts catalog Module

1. Create a YouTube posts catalog interface.
   - Add a new Module under `trr_backend/socials/youtube/posts_catalog/`.
   - Define a small result type with `posts`, `rows`, `retrieval_meta`, `profile_snapshot`, `persist_counters`, and `error_code` fields.
   - The interface should hide whether data came from channel page JSON, Data API identity, yt-dlp enrichment, or continuation fetches.

2. Move posts-only catalog orchestration out of the monolith.
   - Move the orchestration currently inside `_scrape_shared_youtube_posts` into the new YouTube Module.
   - Keep `_scrape_shared_youtube_posts` as a compatibility wrapper that delegates to the new Module.
   - Preserve `youtube_empty_channel_page` behavior and tests.

3. Split YouTube fetching from parsing only where it increases depth.
   - Do not mechanically split `YouTubeScraper` by line count.
   - Extract posts catalog fetching/parsing helpers only if the new Module can expose a smaller interface than `YouTubeScraper.scrape()`.
   - Leave comment parsing, transcript fetching, and media resolving alone unless posts catalog correctness depends on them.

4. Add YouTube posts persistence adapter.
   - Move the payload-building rules from `_upsert_youtube_video` into a YouTube persistence adapter or a shared callable owned by the YouTube Module.
   - Keep `_upsert_youtube_video` as a compatibility wrapper if tests or other code still import it.
   - Preserve optional column checks for `hashtags`, `mentions`, `is_short`, `source_surface`, transcript fields, and `user_avatar_url`.

5. Strengthen YouTube retry and fallback diagnostics.
   - Preserve current `youtube_continuation_fetch_failed` retryability.
   - Ensure `youtube_empty_channel_page`, continuation failures, missing yt-dlp, and Data API-disabled states are represented in one metadata shape.
   - Add tests that assert `runtime_metadata` and `retrieval_meta` are sufficient for admin progress and queue failure displays.

Phase 3: Shared catalog persistence and progress cleanup

1. Reuse one progress interface for TikTok and YouTube catalog persistence.
   - Prefer a platform-local adapter that calls a generic progress helper.
   - Avoid duplicate inline `_emit_persist_progress` functions in `_scrape_shared_tiktok_posts` and `_scrape_shared_youtube_posts`.

2. Evaluate safe batch upsert.
   - For TikTok, compare `_upsert_tiktok_post` payload fields with current `social.tiktok_posts` optional columns.
   - For YouTube, compare `_upsert_youtube_video` payload fields with current `social.youtube_videos` optional columns.
   - Add `_pg_upsert_many` only if the return rows, conflict target, optional columns, and assignment payload behavior are equivalent to the current per-row helpers.

3. Preserve compatibility imports.
   - Update `tests/repositories/test_social_control_plane_imports.py` if ownership moves.
   - Keep old repository-level function names delegating until downstream callers are updated.

## Validation

Run focused backend tests first:

```bash
pytest tests/socials/tiktok/posts_scrapling/test_fetcher.py \
  tests/socials/tiktok/posts_scrapling/test_job_runner.py \
  tests/socials/tiktok/posts_scrapling/test_persistence.py \
  tests/repositories/test_tiktok_posts_scrapling_start.py \
  tests/repositories/test_tiktok_posts_scrapling_lane.py \
  tests/repositories/test_tiktok_posts_scrapling_worker_lane.py
```

Expected result: TikTok posts lane tests pass, including new tests for cancellation, degraded DB handling, page-size bounds, remote auth probe, and unrecoverable failure taxonomy.

```bash
pytest tests/socials/youtube/test_scraper.py \
  tests/socials/youtube/test_media_resolver.py \
  tests/repositories/test_youtube_catalog_backfill_diagnostics.py \
  tests/scripts/test_youtube_scrape_cli.py
```

Expected result: YouTube scrape, metadata, media resolver, empty-page diagnostics, and CLI tests pass.

Run shared social regression tests touched by compatibility wrappers:

```bash
pytest tests/repositories/test_social_control_plane_imports.py \
  tests/repositories/test_social_season_analytics.py \
  tests/repositories/test_social_queue_status.py
```

Expected result: no regressions in social import ownership, queue failure display, or shared catalog behavior. If full `test_social_season_analytics.py` is too slow locally, run the targeted tests that cover `_scrape_shared_tiktok_posts`, `_scrape_shared_youtube_posts`, `_upsert_tiktok_post`, `_upsert_youtube_video`, and shared catalog persistence.

Optional runtime checks after tests:

```bash
python scripts/modal/verify_modal_readiness.py --probe-remote-auth tiktok --json
python -m scripts.socials.tiktok.smoke_posts_scrapling --account bravotv --max-pages 1
python -m scripts.socials.youtube.scrape --channel bravo --max-results 5 --no-comments
```

Expected result: TikTok auth probe returns a structured readiness payload, TikTok smoke returns one-page job metadata, and YouTube CLI still returns channel videos without requiring comments.

## Acceptance Criteria

- TikTok posts `posts_scrapling` jobs can be cancelled cleanly and report a platform-specific cancellation code.
- TikTok posts job failures include `last_error_code`, `last_error_class`, retry decision, fetcher runtime, progress counters, and persistence counters.
- TikTok remote auth probing supports TikTok instead of throwing an unsupported-platform error.
- TikTok page size is configurable by env with bounded defaults.
- YouTube catalog scraping has a platform-owned posts catalog Module or equivalent deep Module; `_scrape_shared_youtube_posts` is no longer the behavior owner.
- YouTube empty-page, continuation failure, yt-dlp availability, Data API identity, profile snapshot, and persistence counters remain visible in retrieval metadata.
- No new TikTok comments ingestion lane is introduced.
- Existing route payloads and DB schema remain unchanged.

## Risks / Open Questions

- TikTok batch upsert may not be safe if `_upsert_tiktok_post` optional-column behavior or assignment payload logic cannot be reproduced exactly. Treat this as an optimization after contract parity, not a blocker for job-runner hardening.
- YouTube extraction can become shallow if it only wraps `YouTubeScraper.scrape()`. Apply the deletion test: if deleting the new Module only reveals the same monolith call, keep extracting until callers can know less.
- YouTube comments already have persisted paths in the monolith. Do not move them during this pass unless a posts-catalog test proves they are coupled to the catalog interface.
- Modal probe changes touch operator readiness behavior. Keep probe payloads backwards-compatible with existing readiness scripts.

## Recommended Handoff

Use `orchestrate-plan-execution` for this plan. The work is sequential because TikTok hardening and YouTube extraction both touch `social_season_analytics_impl.py` compatibility wrappers and shared social tests.

Suggested commit boundaries:

1. TikTok job-runner and auth-probe hardening with focused tests.
2. YouTube posts catalog Module extraction with compatibility wrapper tests.
3. Shared catalog persistence/progress cleanup and optional batch-upsert follow-through.

## Ready For Execution

Ready for execution. Start with Phase 1 because it fixes concrete TikTok bugs and reduces risk before the larger YouTube Module extraction.
