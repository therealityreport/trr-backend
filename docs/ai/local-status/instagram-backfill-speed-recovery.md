# Instagram Backfill Speed Recovery

## Date
- 2026-04-06

## Scope
- Restore fast-path execution for Instagram full-history catalog backfills.
- Remove classification work from the fetch critical path.
- Pin catalog runs to the runtime version they were queued against.
- Add a dedicated benchmark entrypoint for true full-history throughput measurement.

## Backend Changes
- Healthy Instagram full-history backfills now default to `full_history_cursor_breakpoints` with `cursor_breakpoints` partitions instead of `newest_first_frontier`.
- `resume_tail` keeps the frontier path; full-history default no longer does.
- Instagram partition discovery and partition fetch now prefer authenticated GraphQL first and can disable public fallback.
- Partitioned full-history fetch raises immediately when Instagram auth is not available instead of silently degrading to the slow public crawl.
- `post_classify` is no longer enqueued during frontier bootstrap or per-fetch-page progress. Classification is now deferred until the fetch phase is fully complete for the run.
- New jobs are stamped with `required_runtime_version`, and claim selection now filters modal-required jobs to the matching runtime label.
- Run progress exposes `required_runtime_version` and a `runtime_version_pin_mismatch` warning for requeue guidance.

## Benchmarking
- Added `scripts/socials/benchmark_instagram_catalog_full_history.py`.
- The script starts a real Instagram catalog backfill, polls progress to completion, then computes:
  - `total_posts_checked`
  - `total_posts_saved`
  - `pages_scanned`
  - `posts_per_minute`
  - `pages_per_minute`
  - `transport_used`
  - `elapsed_seconds`

## Validation
- `ruff check trr_backend/repositories/social_season_analytics.py tests/repositories/test_social_season_analytics.py scripts/socials/benchmark_instagram_catalog_full_history.py tests/scripts/test_benchmark_instagram_catalog_full_history.py`
- `ruff format --check trr_backend/repositories/social_season_analytics.py tests/repositories/test_social_season_analytics.py scripts/socials/benchmark_instagram_catalog_full_history.py tests/scripts/test_benchmark_instagram_catalog_full_history.py`
- `pytest -q tests/repositories/test_social_season_analytics.py -k 'uses_cursor_breakpoints_for_healthy_instagram_full_history or remote_auth_is_not_ready or maybe_enqueue_shared_catalog_classify_jobs_after_fetch or fetch_shared_instagram_graphql_page or scrape_shared_instagram_posts_partitioned or run_shared_account_discovery_stage or run_shared_account_frontier_posts_stage or claim_next_jobs' tests/scripts/test_benchmark_instagram_catalog_full_history.py`

## Operational Follow-Up
- Deploy backend API and redeploy the Modal worker app before relying on `Backfill Posts`.
- Cancel and requeue any in-flight Instagram backfill that started before this runtime pinning change if it is still running on the old strategy.

## Live Verification
- Deployed `trr_backend.modal_jobs` to Modal and re-verified remote Instagram auth readiness with `scripts/modal/verify_modal_readiness.py --probe-remote-auth instagram --json`.
- Fixed the remaining sync-recent SQL regression in `_shared_post_rows_for_account` by replacing the invalid `created_at` tiebreaker with platform-safe ordering that falls back to `scraped_at`.
- Verified a fresh `sync_recent` canary run completed on the pinned Modal runtime without the previous `UndefinedColumn` failure:
  - run: `e59da8b1-7a48-4677-9944-bc28aca35cc4`
  - runtime: `modal:main · im-MSupLqmKW8pvBp5Wn5lnTa`
- Started a fresh full-history `Backfill Posts` canary on the same pinned runtime:
  - run: `6cf227d4-7d0e-4447-9a4b-f7b55fc6eb1d`
  - strategy: `full_history_cursor_breakpoints`
  - partition strategy: `cursor_breakpoints`
  - runner count: `4`
- The full-history discovery job is actively progressing on the new runtime and has already advanced beyond the initial stalled state seen before this fix.

## Handoff Snapshot
```yaml
handoff:
  include: true
  state: recent
  last_updated: 2026-04-07
  current_phase: "instagram backfill speed recovery deployed and live-verified"
  next_action: "Let run 6cf227d4-7d0e-4447-9a4b-f7b55fc6eb1d continue on the pinned Modal runtime, then decide whether to implement additional throughput optimizations such as larger page size, adaptive catalog delay, and batched upserts."
  detail: self
```
