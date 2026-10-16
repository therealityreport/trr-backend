# Revised Implementation Plan: Threads, Twitter/X, and Facebook Architecture Parity With All Suggestions

Date: 2026-05-05
Scope: `/Users/thomashulihan/Projects/TRR/TRR-Backend`
Source plan: `docs/codex/plans/2026-05-05-threads-twitter-facebook-architecture-parity.md`
Prior package: `.plan-grader/threads-twitter-facebook-architecture-parity-20260505-081358/`
Status: Approved for execution with `orchestrate-subagents`

## Summary

Improve Threads, Twitter/X, and Facebook social scraper architecture by applying the platform-local Module patterns already used for Instagram, TikTok, and YouTube, then fold every prior Plan Grader suggestion into the executable scope.

Target outcome:

- Threads claimed-job posts runs support cancellation, degraded DB finalization/read behavior, safe terminal metadata, and skip-reason counters.
- Twitter/X, Facebook, and Threads remote-auth readiness probes work for every platform listed in `REMOTE_AUTH_REQUIRED_PLATFORMS`.
- Twitter/X, Facebook, and Threads shared-account catalog orchestration lives in platform-local `posts_catalog` Modules while the legacy monolith functions remain compatibility wrappers.
- Additional operator, fixture, import-cycle, smoke, review, and cleanup suggestions are implemented as required plan tasks.
- No DB schema, route payload, scrape stage, worker lane, comments contract, or app-facing contract change is introduced.

## Project Context

Current repo evidence:

- `docs/architecture/social-platform-module-checklist.md` defines the expected platform Module shape.
- `trr_backend/socials/threads/posts_scrapling/` already owns a claimed-job lane with `session.py`, `proxy.py`, `fetcher.py`, `persistence.py`, and `job_runner.py`.
- `trr_backend/socials/threads/jobs.py` registers `threads_posts_scrapling`.
- `trr_backend/socials/threads/posts_scrapling/job_runner.py` needs cancellation and degraded finalization/read handling parity with the hardened Instagram/TikTok lanes.
- `REMOTE_AUTH_REQUIRED_PLATFORMS` in `trr_backend/socials/social_season_analytics_impl.py` includes `twitter`, `facebook`, and `threads`, while the remote auth probe path still needs explicit support for all three.
- Existing cookie loaders and validators can support safe Twitter/X, Facebook, and Threads remote auth probes.
- `_scrape_shared_twitter_posts()`, `_scrape_shared_facebook_posts()`, and `_scrape_shared_threads_posts()` still own shared catalog orchestration in `social_season_analytics_impl.py`.
- `trr_backend/socials/twitter/` and `trr_backend/socials/facebook/` have scraper/auth/fetch support Modules but no `posts_catalog` Modules.
- The worktree is dirty across social scraper, control-plane, docs, and test surfaces. Implementation must preserve unrelated user changes.

## Assumptions

- The current branch is `main`.
- This is a mutation session. No branches or worktrees should be created.
- Existing dirty files are user-owned or prior-session-owned unless this implementation changes them.
- Twitter/X and Facebook work is shared catalog Module extraction plus readiness hardening only.
- Threads has two separate seams: `posts_scrapling` for claimed-job lifecycle and `posts_catalog` for shared-account catalog orchestration.
- Batch upsert remains deferred unless explicit equivalence tests prove it safe.

## Non-Goals And Stop Rules

- Do not add DB tables, migrations, route payload fields, new scrape stages, new API routes, new app UI behavior, or new worker lanes.
- Do not create Twitter/X or Facebook claimed-job lanes, dispatch cases, queue lanes, or worker stages in this pass.
- Do not merge `trr_backend/socials/threads/posts_scrapling/` and `trr_backend/socials/threads/posts_catalog/`.
- Do not move Twitter/X comments, quotes, media mirroring, repair scripts, or `scripts/socials/twitter/scrape.py` persistence semantics.
- Do not move Facebook comments, share detail refresh, media mirror behavior, or `_upsert_facebook_comment_tree()`.
- Do not switch any of the three platforms to batch upsert unless equivalence tests prove payload shape, optional-column gates, conflict target, assignment payload, `job_id`, and return-shape behavior.
- Do not emit raw cookie, bearer token, CSRF token, twikit credential, or proxy secret values in readiness payloads, runtime metadata, fixtures, docs, or failures.
- Do not revert unrelated dirty worktree changes.

## Implementation Changes

### Phase 0: Tests First

Add focused tests before behavior changes where feasible.

Threads claimed-job lane:

- `tests/socials/threads/posts_scrapling/test_job_runner.py::test_threads_job_runner_marks_cancelled_job`
- `tests/socials/threads/posts_scrapling/test_job_runner.py::test_threads_job_runner_marks_cancelled_run`
- `tests/socials/threads/posts_scrapling/test_job_runner.py::test_threads_job_runner_checks_cancellation_after_fetch`
- `tests/socials/threads/posts_scrapling/test_job_runner.py::test_threads_job_runner_returns_degraded_summary_when_final_read_saturated`
- `tests/socials/threads/posts_scrapling/test_job_runner.py::test_threads_job_runner_defers_run_finalization_when_db_saturated`
- `tests/socials/threads/posts_scrapling/test_job_runner.py::test_threads_job_runner_records_terminal_runtime_metadata`
- `tests/socials/threads/posts_scrapling/test_persistence.py::test_persist_threads_posts_counts_skips_by_reason`

Remote auth readiness:

- `tests/repositories/test_social_season_analytics.py::test_probe_remote_auth_health_reports_twitter_safe_structure_flags`
- `tests/repositories/test_social_season_analytics.py::test_probe_remote_auth_health_reports_facebook_safe_structure_flags`
- `tests/repositories/test_social_season_analytics.py::test_probe_remote_auth_health_reports_threads_safe_structure_flags`
- `tests/repositories/test_social_season_analytics.py::test_probe_remote_auth_health_rejects_unsupported_platform`
- `tests/scripts/test_verify_modal_readiness.py::test_parse_args_accepts_twitter_remote_auth_probe`
- `tests/scripts/test_verify_modal_readiness.py::test_parse_args_accepts_facebook_remote_auth_probe`
- `tests/scripts/test_verify_modal_readiness.py::test_parse_args_accepts_threads_remote_auth_probe`
- `tests/scripts/test_verify_modal_readiness.py::test_remote_auth_probe_failure_blocks_strict_readiness`

Shared catalog delegation:

- `tests/repositories/test_social_season_analytics.py::test_scrape_shared_twitter_posts_delegates_to_posts_catalog`
- `tests/repositories/test_social_season_analytics.py::test_scrape_shared_facebook_posts_delegates_to_posts_catalog`
- `tests/repositories/test_social_season_analytics.py::test_scrape_shared_threads_posts_delegates_to_posts_catalog`
- new `tests/socials/twitter/test_twitter_posts_catalog.py`
- new `tests/socials/facebook/test_facebook_posts_catalog.py`
- new `tests/socials/threads/test_threads_posts_catalog.py`

Import and queue status:

- extend `tests/repositories/test_social_control_plane_imports.py` for new `posts_catalog` Modules.
- extend `tests/repositories/test_social_queue_status.py` only if new error codes need queue visibility coverage.

### Phase 1: Extend Remote Auth Readiness

Files:

- `trr_backend/socials/social_season_analytics_impl.py`
- `scripts/modal/verify_modal_readiness.py`
- `tests/repositories/test_social_season_analytics.py`
- `tests/scripts/test_verify_modal_readiness.py`

Tasks:

1. Preserve Instagram and TikTok remote-auth behavior.
2. Extend `probe_remote_auth_health(platform)` for `twitter`, `facebook`, and `threads`.
3. Prefer existing cookie registry helpers where possible.
4. Use platform-specific loaders only where needed for safe structure flags.
5. Return safe booleans and validation summaries only.
6. Allow `--probe-remote-auth twitter`, `facebook`, and `threads` in the readiness CLI.
7. Keep unsupported platform behavior explicit.

Safe flags:

- Twitter/X: `has_auth_token`, `has_ct0`, `has_bearer_token`, `has_twikit_credentials`.
- Facebook: `has_c_user`, `has_xs`.
- Threads: `has_sessionid`, `has_csrftoken`.

### Phase 2: Harden Threads Posts Scrapling Lane

Files:

- `trr_backend/socials/threads/posts_scrapling/job_runner.py`
- `trr_backend/socials/threads/posts_scrapling/persistence.py`
- `trr_backend/socials/threads/posts_scrapling/fetcher.py` only if metadata plumbing requires it.
- `tests/socials/threads/posts_scrapling/*`

Tasks:

1. Add lane-local cancellation support.
2. Check cancellation after session/auth resolution, warmup, fetch, persist, and before final completion.
3. Finish cancelled jobs with `last_error_code="threads_posts_scrapling_cancelled"` and safe cancellation metadata.
4. Catch `pg.DatabaseServiceUnavailableError` around run finalization and final run summary reads.
5. Return a degraded summary when final DB reads are unavailable.
6. Add stable `posts_skipped_by_reason` persistence diagnostics.
7. Include terminal metadata for progress, counters, runtime, fetcher state, persistence state, stop reason, and activity.
8. Preserve safe fingerprints only.
9. Keep legacy `ThreadsScraper` fallback guarded by existing retry policy.

### Phase 3: Extract Twitter/X Posts Catalog Module

Files:

- new `trr_backend/socials/twitter/posts_catalog/__init__.py`
- new `trr_backend/socials/twitter/posts_catalog/catalog.py`
- `trr_backend/socials/social_season_analytics_impl.py`
- `tests/socials/twitter/test_twitter_posts_catalog.py`
- `tests/repositories/test_social_season_analytics.py`

Tasks:

1. Implement `scrape_shared_twitter_posts(...)` in the new Module.
2. Use dependency injection for monolith helpers, auth access, scraper access, and persistence callbacks.
3. Preserve full-history window policy, page-limit policy, reply filtering, profile snapshot, progress counters, `last_retrieval_meta`, and wrapper return shape.
4. Keep `_scrape_shared_twitter_posts()` as the compatibility wrapper.
5. Do not add Twitter/X worker-lane behavior.

### Phase 4: Extract Facebook Posts Catalog Module

Files:

- new `trr_backend/socials/facebook/posts_catalog/__init__.py`
- new `trr_backend/socials/facebook/posts_catalog/catalog.py`
- `trr_backend/socials/social_season_analytics_impl.py`
- `tests/socials/facebook/test_facebook_posts_catalog.py`
- `tests/repositories/test_social_season_analytics.py`

Tasks:

1. Implement `scrape_shared_facebook_posts(...)` in the new Module.
2. Use dependency injection for scraper, document-fetch, profile, and persistence callbacks.
3. Preserve `FacebookScrapeConfig` defaults, delay env behavior, max scroll env behavior, profile snapshot, `last_retrieval_meta`, shared catalog progress, and wrapper return shape.
4. Keep `_scrape_shared_facebook_posts()` as the compatibility wrapper.
5. Do not add Facebook worker-lane behavior.

### Phase 5: Extract Threads Shared Catalog Module

Files:

- new `trr_backend/socials/threads/posts_catalog/__init__.py`
- new `trr_backend/socials/threads/posts_catalog/catalog.py`
- `trr_backend/socials/social_season_analytics_impl.py`
- `tests/socials/threads/test_threads_posts_catalog.py`
- `tests/repositories/test_social_season_analytics.py`

Tasks:

1. Implement `scrape_shared_threads_posts(...)` in the new Module.
2. Keep this Module separate from `threads/posts_scrapling`.
3. Preserve `ThreadsScrapeConfig`, env delay behavior, profile snapshot, `last_retrieval_meta`, shared catalog progress, existing persistence callbacks, and wrapper return shape.
4. Keep `_scrape_shared_threads_posts()` as the compatibility wrapper.
5. Do not route shared catalog work through the claimed-job lifecycle lane.

### Phase 6: Operator Fixtures, Docs, And Final Validation

Files:

- `tests/fixtures/socials/run_metadata/`
- `docs/runbooks/social_worker_queue_ops.md`
- `docs/architecture/social-platform-module-checklist.md`
- `docs/architecture/social-compatibility-wrapper-ledger.md`
- `tests/repositories/test_social_control_plane_imports.py`
- `tests/repositories/test_social_queue_status.py`

Tasks:

1. Add representative metadata fixtures for Threads cancellation/degraded cases and Twitter/Facebook/Threads remote-auth failures.
2. Validate fixtures contain no raw secret values.
3. Update queue ops docs with readiness commands, smoke commands, error-code interpretation, and no-new-lane notes.
4. Update the architecture checklist with current review points for Twitter/X, Facebook, and Threads.
5. Update the compatibility wrapper ledger with owner Module and deletion criteria for the wrappers left in `social_season_analytics_impl.py`.
6. Add a future batch-upsert equivalence checklist while keeping batch upsert deferred.

## ADDITIONAL SUGGESTIONS

### Task 1: Source Suggestion 1 - Remote Auth Smoke Wrapper

- Concrete changes: add or extend a small smoke wrapper that runs `twitter`, `facebook`, and `threads` remote-auth probes after focused tests pass.
- Dependencies or ordering: after Phase 1.
- Affected files or surfaces: prefer `scripts/socials/` or an existing smoke script path; document commands in `docs/runbooks/social_worker_queue_ops.md`.
- Validation and expected result: smoke command reaches the readiness probe entrypoints and returns structured JSON without raw secrets.
- Acceptance criteria: one operator-copyable smoke command covers all three platforms or three explicit platform commands are documented.
- Commit boundary: include with readiness/docs changes, not with platform catalog extraction.

### Task 2: Source Suggestion 2 - Run Metadata Fixture Secret Validator

- Concrete changes: add a generated fixture validator that scans `tests/fixtures/socials/run_metadata/` for raw cookie/token-like keys and values.
- Dependencies or ordering: after fixtures are added in Phase 6.
- Affected files or surfaces: `tests/fixtures/socials/run_metadata/`, a test or helper under `tests/`, and possibly `tests/scripts/`.
- Validation and expected result: validator passes on new fixtures and fails on a synthetic raw-secret fixture.
- Acceptance criteria: fixture validation is automated by pytest.
- Commit boundary: include with fixture updates.

### Task 3: Source Suggestion 3 - Compatibility Wrapper Ledger Entries

- Concrete changes: add ledger entries for `_scrape_shared_twitter_posts()`, `_scrape_shared_facebook_posts()`, and `_scrape_shared_threads_posts()` including owner Module and deletion criteria.
- Dependencies or ordering: after Phases 3-5 wrappers are wired.
- Affected files or surfaces: `docs/architecture/social-compatibility-wrapper-ledger.md`.
- Validation and expected result: ledger names the new owner Module for each wrapper.
- Acceptance criteria: every remaining wrapper changed by this plan has a ledger entry.
- Commit boundary: include with docs pass.

### Task 4: Source Suggestion 4 - Operator Query Snippet

- Concrete changes: add a one-page query snippet for recent `twitter`, `facebook`, and `threads` remote-auth failures by `last_error_code`.
- Dependencies or ordering: after Phase 1 error codes and metadata shape are final.
- Affected files or surfaces: `docs/runbooks/social_worker_queue_ops.md` or a linked local-status/runbook snippet.
- Validation and expected result: snippet references existing tables/fields only.
- Acceptance criteria: operator can copy the query without needing code context.
- Commit boundary: include with docs pass.

### Task 5: Source Suggestion 5 - Expanded Posts Catalog Import-Cycle Checks

- Concrete changes: expand import-cycle checks beyond the three new Modules to cover all current `posts_catalog` Modules.
- Dependencies or ordering: after Phases 3-5.
- Affected files or surfaces: `tests/repositories/test_social_control_plane_imports.py`.
- Validation and expected result: all `posts_catalog` Modules import without circular dependency failures.
- Acceptance criteria: pytest import test covers every current posts catalog package.
- Commit boundary: include with shared validation tests.

### Task 6: Source Suggestion 6 - Reusable Fake Persistence Adapter

- Concrete changes: add a reusable fake persistence adapter for platform posts catalog tests.
- Dependencies or ordering: before or during Phases 3-5 tests.
- Affected files or surfaces: `tests/socials/`, platform posts catalog tests, or a small helper under `tests/socials/helpers/`.
- Validation and expected result: Twitter/X, Facebook, and Threads posts catalog tests use the same helper shape where practical.
- Acceptance criteria: no platform creates a divergent fake persistence contract without a documented reason.
- Commit boundary: include with catalog tests.

### Task 7: Source Suggestion 7 - Catalog Metadata Golden Fixtures

- Concrete changes: add one catalog metadata golden fixture per platform after the first Module implementation lands.
- Dependencies or ordering: after each platform catalog returns stable metadata.
- Affected files or surfaces: `tests/fixtures/socials/run_metadata/` or a nearby platform fixture folder.
- Validation and expected result: tests assert stable metadata field names for Twitter/X, Facebook, and Threads.
- Acceptance criteria: one golden metadata fixture exists per platform.
- Commit boundary: include with platform catalog tests or fixture pass.

### Task 8: Source Suggestion 8 - Conditional Narrow Benchmark

- Concrete changes: add a narrow benchmark only if catalog extraction materially changes runtime path length.
- Dependencies or ordering: after Phases 3-5 and only if code review finds a meaningful path-length change.
- Affected files or surfaces: benchmark file under `tests/` or `scripts/` only when needed.
- Validation and expected result: if added, benchmark is narrow and does not replace correctness tests.
- Acceptance criteria: either a benchmark exists with a documented reason or `VALIDATION.md`/runbook notes no material path-length change found.
- Commit boundary: separate from correctness implementation if added.

### Task 9: Source Suggestion 9 - Batch-Upsert Equivalence Checklist

- Concrete changes: add a future checklist for Twitter/X, Facebook, and Threads batch-upsert equivalence.
- Dependencies or ordering: after persistence behavior is preserved.
- Affected files or surfaces: `docs/architecture/social-platform-module-checklist.md` or a dedicated architecture note.
- Validation and expected result: checklist covers payload shape, optional columns, conflict target, assignments, `job_id`, and return shape.
- Acceptance criteria: docs explicitly state batch upsert remains deferred.
- Commit boundary: include with architecture docs pass.

### Task 10: Source Suggestion 10 - Compatibility Wrapper Cleanup Plan

- Concrete changes: add a follow-up cleanup plan for retiring compatibility wrappers after enough direct Module callers exist.
- Dependencies or ordering: after wrapper ledger entries are updated.
- Affected files or surfaces: `docs/architecture/social-compatibility-wrapper-ledger.md` or `docs/codex/plans/`.
- Validation and expected result: cleanup plan references current wrapper names and owner Modules.
- Acceptance criteria: future cleanup criteria are explicit but not executed in this pass.
- Commit boundary: include with docs pass.

### Task 11: Source Suggestion 11 - Modal Readiness Smoke Command Section

- Concrete changes: add a copyable Modal readiness smoke command section for Twitter/X, Facebook, and Threads.
- Dependencies or ordering: after Phase 1 CLI support.
- Affected files or surfaces: `docs/runbooks/social_worker_queue_ops.md`.
- Validation and expected result: commands match actual CLI flags.
- Acceptance criteria: runbook has a copyable command block.
- Commit boundary: include with readiness/docs pass.

### Task 12: Source Suggestion 12 - Review Checklist No-New-Lane Question

- Concrete changes: add a review checklist item asking whether the change added a worker lane for Twitter/X or Facebook.
- Dependencies or ordering: any time before final docs validation.
- Affected files or surfaces: `docs/architecture/social-platform-module-checklist.md` or wrapper ledger.
- Validation and expected result: checklist is visible in the architecture docs used for platform parity reviews.
- Acceptance criteria: docs explicitly ask: `Did this change add a worker lane?` for Twitter/X and Facebook.
- Commit boundary: include with architecture docs pass.

## Validation

Focused readiness validation:

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

Threads lane validation:

```bash
pytest tests/socials/threads/posts_scrapling/test_fetcher.py \
  tests/socials/threads/posts_scrapling/test_job_runner.py \
  tests/socials/threads/posts_scrapling/test_persistence.py \
  tests/socials/threads/posts_scrapling/test_proxy.py \
  tests/repositories/test_threads_posts_scrapling_lane.py \
  tests/scripts/test_social_worker.py
```

Platform catalog validation:

```bash
pytest tests/socials/twitter/test_twitter_posts_catalog.py \
  tests/socials/facebook/test_facebook_posts_catalog.py \
  tests/socials/threads/test_threads_posts_catalog.py \
  tests/repositories/test_social_season_analytics.py::test_scrape_shared_twitter_posts_delegates_to_posts_catalog \
  tests/repositories/test_social_season_analytics.py::test_scrape_shared_facebook_posts_delegates_to_posts_catalog \
  tests/repositories/test_social_season_analytics.py::test_scrape_shared_threads_posts_delegates_to_posts_catalog
```

Final shared validation:

```bash
pytest tests/scripts/test_verify_modal_readiness.py \
  tests/repositories/test_social_control_plane_imports.py \
  tests/repositories/test_social_queue_status.py
```

Existing regression validation:

```bash
pytest tests/socials/test_twitter_query_building.py \
  tests/socials/test_twitter_rate_limiting.py \
  tests/socials/test_twitter_runtime_metadata.py \
  tests/scripts/test_twitter_scrape_cli.py \
  tests/scripts/test_twitter_scrape_persist.py \
  tests/socials/test_facebook_document_fetch.py \
  tests/socials/test_facebook_engagement.py \
  tests/api/routers/test_socials_facebook.py \
  tests/repositories/test_social_mirror_repairs.py \
  tests/socials/test_threads_scraper.py
```

Optional operator smoke after tests:

```bash
python scripts/modal/verify_modal_readiness.py --probe-remote-auth twitter --json
python scripts/modal/verify_modal_readiness.py --probe-remote-auth facebook --json
python scripts/modal/verify_modal_readiness.py --probe-remote-auth threads --json
```

## Acceptance Criteria

- Every prior numbered suggestion is represented as a concrete task in `ADDITIONAL SUGGESTIONS`.
- Threads posts Scrapling jobs can be cancelled cleanly.
- Threads posts Scrapling jobs return a degraded summary when final DB reads are unavailable.
- Threads run finalization handles DB saturation without masking the primary result.
- Threads terminal metadata includes actionable progress, runtime, persistence, skip-reason, and error fields without raw secrets.
- `probe_remote_auth_health()` supports Twitter/X, Facebook, and Threads with safe structure flags.
- `scripts/modal/verify_modal_readiness.py` accepts `--probe-remote-auth twitter`, `facebook`, and `threads`.
- Twitter/X shared catalog behavior is owned by `trr_backend/socials/twitter/posts_catalog/`, with `_scrape_shared_twitter_posts()` preserved as a compatibility wrapper.
- Facebook shared catalog behavior is owned by `trr_backend/socials/facebook/posts_catalog/`, with `_scrape_shared_facebook_posts()` preserved as a compatibility wrapper.
- Threads shared catalog behavior is owned by `trr_backend/socials/threads/posts_catalog/`, with `threads/posts_scrapling` kept separate.
- Operator docs include remote auth query snippets, smoke commands, wrapper ledger entries, batch-upsert checklist, cleanup plan, and no-new-lane review question.
- No new comments lane, DB schema, route payload, scrape stage, app contract, or Twitter/Facebook worker lane is introduced.
- Focused validation commands pass or failures are documented with unrelated-failure evidence.

## Risks / Open Questions

- The dirty worktree overlaps target files; every executor must inspect diffs before editing.
- `social_season_analytics_impl.py` is a shared integration surface and should stay under main-session ownership.
- Catalog extraction can become shallow if new Modules only forward the monolith without reducing caller knowledge.
- Runtime-path benchmark work should remain conditional; correctness tests and metadata parity come first.
- Queue status tests may already cover metadata visibility. Add only narrow coverage if current tests lack the new error-code path.

## Recommended Handoff

Use `orchestrate-subagents`.

Main session owns:

- plan revision artifact,
- branch/worktree preflight,
- remote auth readiness,
- `social_season_analytics_impl.py` wrapper integration,
- readiness CLI changes,
- docs, fixtures, import-cycle checks, smoke docs, and final validation.

Worker A owns Threads claimed-job hardening.
Worker B owns Twitter/X posts catalog Module and tests.
Worker C owns Facebook posts catalog Module and tests.
Worker D owns Threads shared catalog Module and tests.

Workers must not create branches or worktrees, must not revert unrelated changes, and must report changed files and validations.

## Ready For Execution

Ready for execution after branch `main` and dirty worktree preflight are confirmed. Start with Phase 1 remote auth in the main session while subagents work on disjoint platform-local Modules.

## Archive Plan

After this plan is completely implemented and verified, archive the active plan instead of leaving it in the active planning path. Move or mark the canonical plan and this `REVISED_PLAN.md` as completed, implemented, failed, or superseded with evidence, so future planning does not treat completed or failed work as an untried plan.

## Cleanup Note

After this plan is completely implemented and verified, delete any temporary planning artifacts that are no longer needed, including generated audit, scorecard, suggestions, comparison, patch, benchmark, and validation files. Do not delete them before implementation is complete because they are part of the execution evidence trail.
