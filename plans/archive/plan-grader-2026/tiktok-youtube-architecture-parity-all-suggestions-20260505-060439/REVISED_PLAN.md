# Revised Plan: TikTok and YouTube Social Architecture Parity With All Suggestions

Date: 2026-05-05
Source plan: `docs/codex/plans/2026-05-05-tiktok-youtube-architecture-parity.md`
Prior package: `.plan-grader/tiktok-youtube-architecture-parity-20260505-055823/`
Scope: `/Users/thomashulihan/Projects/TRR/TRR-Backend`

## Summary

Apply the proven Instagram social-scraper hardening patterns to TikTok and YouTube without creating new DB schema, route payloads, scrape stages, or worker lanes. The implementation must improve operator-facing truth for TikTok posts and YouTube catalog runs: cancellation, retryability, auth readiness, progress, runtime metadata, and failure diagnostics should match what the backend can actually prove.

All ten prior `SUGGESTIONS.md` items are accepted requirements in this revision. They are integrated as concrete work under `ADDITIONAL SUGGESTIONS`, not left as optional notes.

Success is measurable when:

- TikTok `posts_scrapling` cancellation, degraded DB, metadata, and page-size tests pass.
- TikTok remote auth probing returns a structured TikTok readiness payload instead of an unsupported-platform error.
- YouTube shared catalog scraping delegates out of `social_season_analytics_impl.py` into a YouTube-owned posts catalog module while preserving the existing compatibility wrapper.
- Social run metadata fixtures, safe metadata snapshots, error-code coverage, runbook updates, and smoke checks cover the new operational contract.
- Focused TikTok, YouTube, readiness, import, and queue regression tests pass or produce unrelated-failure evidence.

## Project Context

- `docs/architecture/social-platform-module-checklist.md` defines the intended platform module shape and blocks TikTok comments ingestion until comments contract evidence exists.
- `trr_backend/socials/tiktok/posts_scrapling/job_runner.py` already owns TikTok posts orchestration but lacks Instagram-style cancellation and degraded final DB handling.
- `trr_backend/socials/tiktok/posts_scrapling/fetcher.py` hardcodes `TIKTOK_POST_PAGE_SIZE = 30`.
- `probe_remote_auth_health()` in `trr_backend/socials/social_season_analytics_impl.py` only supports Instagram, while TikTok is already represented in worker auth capabilities and Modal probe entrypoints.
- `_scrape_shared_youtube_posts()` currently owns YouTube shared catalog orchestration in `trr_backend/socials/social_season_analytics_impl.py`.
- `trr_backend/socials/youtube/` has scraper, API client, Crawlee adapter, and media resolver modules, but no posts catalog ownership module.
- The backend worktree is dirty across social-control-plane, Instagram, TikTok, Modal readiness, and test files. Executors must inspect current diffs and preserve unrelated work.

## Assumptions

- Current branch remains `main`; no branch or worktree is created.
- Backend route payloads and DB schema stay unchanged.
- Existing dirty files are user-owned or unknown unless this implementation edits them directly.
- TikTok and YouTube comments work remains out of scope.
- Batch upsert is allowed only after contract equivalence is proven by tests.

## Non-Goals

- Do not add a TikTok comments ingestion lane.
- Do not add a YouTube queue stage, worker lane, route payload, table, or migration.
- Do not rewrite unrelated Instagram scraper behavior.
- Do not remove compatibility wrappers until import tests prove callers are updated.
- Do not make app-facing changes unless a backend payload change is discovered and re-planned.

## Implementation Changes

### Phase 0: Preflight And Test Anchors

1. Confirm branch and dirty ownership.
   - Run `git branch --show-current` and `git status --short`.
   - Expected result: branch is `main`; dirty files are classified and not reverted.

2. Add or update focused tests before behavior changes where feasible.
   - TikTok cancellation and degraded summary coverage in `tests/socials/tiktok/posts_scrapling/test_job_runner.py`.
   - TikTok page-size env coverage in `tests/socials/tiktok/posts_scrapling/test_fetcher.py`.
   - TikTok remote auth readiness coverage in `tests/scripts/test_verify_modal_readiness.py` and the closest control-plane probe tests.
   - YouTube wrapper delegation coverage in `tests/repositories/test_youtube_catalog_backfill_diagnostics.py`.

3. Capture expected pre-fix failures in test names or comments only where they clarify intent.
   - Do not add brittle assertions against incidental implementation details.

### Phase 1: Harden TikTok Posts Lane

1. Add cancellation support in `trr_backend/socials/tiktok/posts_scrapling/job_runner.py`.
   - Add a lane-local cancellation exception and cancellation helper modeled on Instagram posts.
   - Check job and run status after warmup, after secUid resolution, before each page fetch, after each persistence call, and before final completion.
   - Finish cancelled work with `last_error_code="tiktok_posts_scrapling_cancelled"` plus `cancel_scope`, `job_status`, `run_status`, and safe runtime metadata.

2. Add degraded DB handling.
   - Catch `pg.DatabaseServiceUnavailableError` around `lifecycle.finalize_run_status(run_id)`.
   - Catch the final job-row read and return a degraded summary with stable fields when the DB is saturated.

3. Improve terminal metadata.
   - Track skipped posts and skipped-by-reason from persistence where available.
   - Include `listing_progress`, `stage_counters`, `persist_counters`, `fetcher_runtime`, `runtime_metadata`, `stop_reason`, and `activity` on success and failure.
   - Keep `last_error_code` and `last_error_class` on failed or retrying jobs.

4. Make page size bounded and env-driven.
   - Add `tiktok_posts_scrapling_page_size()` in `fetcher.py`.
   - Read `SOCIAL_TIKTOK_POSTS_SCRAPLING_PAGE_SIZE`, default to `30`, and bound to `10..50` unless current TikTok evidence requires tighter bounds.
   - Preserve explicit `count=` overrides.

5. Preserve direct API failure taxonomy.
   - Keep auth/challenge failures terminal for direct TikTok API.
   - Keep yt-dlp-to-API fallback limited to the existing allowed failure reasons unless re-planned.

### Phase 2: Add TikTok Remote Auth Readiness

1. Extend `probe_remote_auth_health(platform)`.
   - Keep Instagram behavior unchanged.
   - Add TikTok support using existing `_load_tiktok_cookies_from_sources()` and `_validate_tiktok_cookie_health()`.
   - Return safe fields only, such as `platform`, `ready`, `reason`, `has_sessionid`, `has_sid_tt`, and `has_ms_token`.

2. Update Modal readiness script behavior.
   - Allow `python scripts/modal/verify_modal_readiness.py --probe-remote-auth tiktok --json`.
   - Add tests proving TikTok is accepted and unsupported platforms remain rejected.

### Phase 3: Extract YouTube Posts Catalog Module

1. Create `trr_backend/socials/youtube/posts_catalog/`.
   - Add `__init__.py`.
   - Add `catalog.py` as the posts catalog orchestration owner.
   - Add `types.py` or `persistence.py` only when they reduce coupling and pass tests.

2. Move posts-only orchestration out of the monolith.
   - Move the body of `_scrape_shared_youtube_posts()` into `posts_catalog/catalog.py` as `scrape_shared_youtube_posts(...)`.
   - Keep `_scrape_shared_youtube_posts()` in `social_season_analytics_impl.py` as a compatibility wrapper returning the same shape.
   - Preserve `youtube_empty_channel_page`, `ytdlp_available`, profile snapshot, canonical handle, `retrieval_meta`, and `persist_counters`.

3. Do not create a YouTube worker lane.
   - Do not add `trr_backend/socials/youtube/jobs.py`.
   - Do not register a new worker handler or scrape stage.

4. Persistence adapter rule.
   - First pass may accept compatibility-wrapper callbacks for shared catalog and video upsert.
   - Move payload building only if circular imports and optional-column behavior stay stable.

### Phase 4: Shared Catalog Progress And Optional Batch Upsert

1. Reuse one progress interface where safe.
   - Prefer an existing social/account catalog progress helper if one fits.
   - Keep payload fields stable: `phase`, `pages_scanned`, `posts_checked`, `matched_posts`, `saved_posts`, and optional `total_posts`.

2. Gate batch upsert.
   - Compare TikTok and YouTube payloads against `_upsert_tiktok_post()` and `_upsert_youtube_video()`.
   - Verify conflict columns, optional columns, assignment payloads, returned row fields, and side effects.
   - If equivalent, add platform-local batch tests and use `_pg_upsert_many`.
   - If not equivalent, keep the per-row path and document the blocker in `docs/architecture/social-platform-module-checklist.md`.

3. Preserve import ownership.
   - Update `tests/repositories/test_social_control_plane_imports.py` only if ownership changes.
   - Keep compatibility wrappers until callers and tests prove they are no longer needed.

## ADDITIONAL SUGGESTIONS

### Task 1: Source 1, Add Social Run Metadata Fixtures

- Concrete changes: add or extend fixture coverage for TikTok and YouTube run metadata shapes.
- Dependencies or ordering: after Phase 1 and Phase 3 metadata shapes are known.
- Affected files or surfaces: `tests/fixtures/socials/` or colocated test fixtures if that directory does not exist.
- Validation and expected result: queue/status or repository tests can load fixture metadata and assert required keys without live DB access.
- Acceptance criteria: fixture covers success, failure, cancellation, and degraded-summary shapes for the touched platforms where practical.
- Commit boundary: include with the implementation slice that introduces the metadata shape.

### Task 2: Source 2, Add A One-Page Operator Runbook

- Concrete changes: document how to interpret TikTok and YouTube scraper error codes, readiness probes, smoke commands, and batch-upsert decision status.
- Dependencies or ordering: after error codes and smoke commands are final.
- Affected files or surfaces: `docs/runbooks/social_worker_queue_ops.md` or the closest existing social operations runbook.
- Validation and expected result: markdown exists, references runnable commands, and does not claim schema or route changes.
- Acceptance criteria: an operator can map new metadata keys to next actions without reading source code.
- Commit boundary: docs follow-up after tests pass.

### Task 3: Source 3, Add Error-Code Registry Tests

- Concrete changes: add tests ensuring new TikTok and YouTube error codes remain recognized by queue/status diagnostics.
- Dependencies or ordering: after new error codes are introduced.
- Affected files or surfaces: `tests/repositories/test_social_queue_status.py` and related queue/status code.
- Validation and expected result: tests pass and fail if new codes regress to generic display.
- Acceptance criteria: `tiktok_posts_scrapling_cancelled`, degraded DB metadata, TikTok auth probe failures, `youtube_empty_channel_page`, and continuation failures are represented consistently.
- Commit boundary: include with queue/status metadata changes.

### Task 4: Source 4, Add YouTube Catalog Golden Fixture

- Concrete changes: add a small YouTube channel/page fixture or fixture builder for catalog extraction tests.
- Dependencies or ordering: during Phase 3, before or alongside wrapper delegation tests.
- Affected files or surfaces: `tests/fixtures/socials/youtube/` or an existing YouTube fixture path.
- Validation and expected result: YouTube catalog tests can assert parsing/delegation without live network.
- Acceptance criteria: fixture is small, deterministic, and covers at least one normal video plus an empty-page diagnostic path if practical.
- Commit boundary: include with YouTube module extraction.

### Task 5: Source 5, Add TikTok Fetcher Runtime Metadata Snapshot

- Concrete changes: add a test snapshot or strict key assertion for safe TikTok runtime metadata.
- Dependencies or ordering: after Phase 1 metadata keys are finalized.
- Affected files or surfaces: `tests/socials/tiktok/posts_scrapling/test_fetcher.py` or `test_job_runner.py`.
- Validation and expected result: tests prove cookie/proxy secrets are not leaked and required safe keys remain present.
- Acceptance criteria: metadata contains useful diagnostics and excludes raw cookie values or bearer/session secrets.
- Commit boundary: include with TikTok metadata hardening.

### Task 6: Source 6, Add A Local Smoke Wrapper

- Concrete changes: add a lightweight script that runs or prints the approved TikTok and YouTube post-path smoke checks.
- Dependencies or ordering: after command names and arguments are verified.
- Affected files or surfaces: `scripts/socials/` and optional script tests.
- Validation and expected result: script help or dry-run mode works locally without network credentials.
- Acceptance criteria: operators have one discoverable command for TikTok and YouTube smoke checks.
- Commit boundary: scripts/docs follow-up after core code tests pass.

### Task 7: Source 7, Document Batch-Upsert Decision

- Concrete changes: record whether TikTok/YouTube batch upsert was implemented or deferred, with evidence.
- Dependencies or ordering: after Phase 4 contract-equivalence check.
- Affected files or surfaces: `docs/architecture/social-platform-module-checklist.md`.
- Validation and expected result: docs state the decision and do not imply unimplemented batch behavior.
- Acceptance criteria: future agents can see why batch upsert is safe or why it was deferred.
- Commit boundary: include with Phase 4.

### Task 8: Source 8, Add Import-Cycle Check

- Concrete changes: add or strengthen import tests for YouTube posts catalog extraction and monolith compatibility wrappers.
- Dependencies or ordering: after Phase 3 module files exist.
- Affected files or surfaces: `tests/repositories/test_social_control_plane_imports.py`.
- Validation and expected result: import tests pass and catch circular import regressions.
- Acceptance criteria: importing repository compatibility surfaces and the new YouTube posts catalog module succeeds in isolation.
- Commit boundary: include with YouTube extraction.

### Task 9: Source 9, Add Queue Dashboard Copy Check

- Concrete changes: if backend payloads change, add backend contract evidence for queue/admin copy and create an app follow-through note. If payloads do not change, add a test or validation note proving no payload-copy change is required.
- Dependencies or ordering: after metadata and queue/status tests settle.
- Affected files or surfaces: backend queue tests now; TRR-APP only if API payload changes.
- Validation and expected result: backend queue/status tests prove the new metadata remains coherent for admin display.
- Acceptance criteria: no app-facing mismatch is introduced silently.
- Commit boundary: include with queue/status validation or defer to a separate app plan if API payloads change.

### Task 10: Source 10, Add 30-Day Follow-Up Query

- Concrete changes: add a lightweight SQL snippet or documented query to audit recent TikTok/YouTube failures for actionable metadata.
- Dependencies or ordering: after final metadata keys are known.
- Affected files or surfaces: runbook or `docs/architecture/social-platform-module-checklist.md`.
- Validation and expected result: query is syntactically plausible and references existing social run/job tables.
- Acceptance criteria: the revised plan's 30-day value claim can be checked later.
- Commit boundary: docs follow-up after implementation validation.

## Validation

Run focused TikTok tests:

```bash
pytest tests/socials/tiktok/posts_scrapling/test_fetcher.py \
  tests/socials/tiktok/posts_scrapling/test_job_runner.py \
  tests/socials/tiktok/posts_scrapling/test_persistence.py \
  tests/repositories/test_tiktok_posts_scrapling_start.py \
  tests/repositories/test_tiktok_posts_scrapling_lane.py \
  tests/repositories/test_tiktok_posts_scrapling_worker_lane.py
```

Run YouTube tests:

```bash
pytest tests/socials/youtube/test_scraper.py \
  tests/socials/youtube/test_media_resolver.py \
  tests/repositories/test_youtube_catalog_backfill_diagnostics.py \
  tests/scripts/test_youtube_scrape_cli.py
```

Run shared readiness/import/queue tests:

```bash
pytest tests/scripts/test_verify_modal_readiness.py \
  tests/repositories/test_social_control_plane_imports.py \
  tests/repositories/test_social_queue_status.py
```

Run docs/script targeted checks if files were added:

```bash
python scripts/socials/smoke_tiktok_youtube_posts.py --help
```

Optional smoke checks after unit tests:

```bash
python scripts/modal/verify_modal_readiness.py --probe-remote-auth tiktok --json
python -m scripts.socials.tiktok.smoke_posts_scrapling --account bravotv --max-pages 1
python -m scripts.socials.youtube.scrape --channel bravo --max-results 5 --no-comments
```

Expected result: focused tests pass, or unrelated failures are documented with exact failing node IDs and error text. Optional smoke checks return structured output without generic unsupported-platform or missing-metadata failures.

## Acceptance Criteria

- New plan artifact includes all ten prior suggestions as concrete tasks.
- TikTok posts jobs support cancellation and degraded DB finalization.
- TikTok posts job metadata includes actionable progress, runtime, persistence, and error fields without leaking secrets.
- TikTok remote auth probe supports TikTok locally and through Modal readiness.
- YouTube shared catalog behavior is owned by `trr_backend/socials/youtube/posts_catalog/`, with a compatibility wrapper in the monolith.
- Error-code, import-cycle, fixture, smoke-wrapper, runbook, and 30-day query tasks are implemented or documented with evidence-backed stop reasons.
- Existing TikTok and YouTube route/job contracts remain unchanged.
- No TikTok comments ingestion lane is introduced.
- Focused validation commands pass or failures are documented with unrelated-failure evidence.

## Stop Rules

- Stop and re-plan if implementation needs a new table, route payload, scrape stage, worker lane, run status, or app-facing response contract.
- Stop if YouTube extraction creates circular imports that require broad monolith helper movement.
- Stop if TikTok or YouTube batch upsert cannot preserve current row shape or optional-column behavior.
- Stop TikTok comments work unless the comments contract evidence in `docs/architecture/social-platform-module-checklist.md` is satisfied.
- Stop before editing TRR-APP unless backend API behavior changes and the user approves an app follow-through.

## Recommended Handoff

Use `orchestrate-subagents`.

- Worker A owns TikTok posts lane hardening and TikTok-focused tests.
- Worker B owns YouTube posts catalog extraction, YouTube fixture coverage, and import-cycle checks.
- Worker C owns docs/scripts/fixture follow-through for accepted suggestions that do not overlap Worker A or B.
- Main session owns shared integration files, TikTok remote auth readiness, queue/status contract checks, final validation, and conflict resolution.

Workers are not alone in the codebase. They must not create branches or worktrees, must not revert unrelated dirty worktree changes, and must adapt to concurrent edits.

## Archive Plan

After this plan is completely implemented and verified, archive the active plan instead of leaving it in the active planning path. Move or mark the canonical plan and this `REVISED_PLAN.md` as completed, implemented, failed, or superseded with evidence, so future planning does not treat completed or failed work as an untried plan.

## Cleanup Note

After this plan is completely implemented and verified, delete any temporary planning artifacts that are no longer needed, including generated audit, scorecard, suggestions, comparison, patch, benchmark, and validation files. Do not delete them before implementation is complete because they are part of the execution evidence trail.

## Ready For Execution

Ready for execution after the orchestrator confirms the branch is `main`, preserves unrelated dirty files, and assigns disjoint subagent write scopes.
