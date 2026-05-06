# Social Scraper Separation Phase 1 Inventory

## Handoff Snapshot
```yaml
handoff:
  include: true
  state: recent
  last_updated: 2026-05-02
  current_phase: "phase 0–1.5 inventory captured; baseline 65114 lines and dirty worktree overlap recorded"
  next_action: "Resolve overlapping Instagram comments/cookie dirty files before resuming runtime moves out of social_season_analytics.py."
  detail: self
```

Date: 2026-05-02

Plan source: `/Users/thomashulihan/Projects/TRR/.plan-grader/social-scraper-separation-plan-20260502-142100/REVISED_PLAN.md`

Scope: Phase 0 through Phase 1.5 only. No runtime implementation moved in this pass.

## Baseline

- Legacy hub: `trr_backend/repositories/social_season_analytics.py`
- Baseline size observed before this pass: `65114` lines.
- `python -m compileall -q trr_backend/repositories/social_season_analytics.py trr_backend/socials` passed during Plan Grader validation.

## Dirty Worktree Blocker

`TRR-Backend` currently has overlapping dirty scraper files that are not part of this separation pass:

- `tests/repositories/test_social_season_analytics.py`
- `tests/socials/test_cookie_refresh_flows.py`
- `tests/socials/test_instagram_comments_scrapling_retry.py`
- `trr_backend/repositories/social_season_analytics.py`
- `trr_backend/socials/instagram/comments_scrapling/fetcher.py`
- `trr_backend/socials/instagram/cookie_refresh.py`
- `docs/ai/local-status/instagram-comments-schema-preflight-20260501-220456.txt`

Treat these as active Instagram comments/cookie work until assigned otherwise. Do not move runtime implementations out of `social_season_analytics.py` while these overlap remains unresolved.

## Current Canonical Owners Already Extracted

These functions are currently exported through `trr_backend.socials.control_plane` and are not the same object as the legacy hub implementation:

| Surface | Canonical module | Legacy compatibility |
| --- | --- | --- |
| queue status | `trr_backend/socials/control_plane/queue_status.py` | legacy `get_queue_status` remains importable |
| run reads | `trr_backend/socials/control_plane/run_reads.py` | legacy `list_runs`, `list_run_summaries`, `get_run_progress_snapshot` remain importable |
| shared status reads | `trr_backend/socials/control_plane/shared_status_reads.py` | legacy `get_season_shared_status`, `list_shared_runs` remain importable |
| run lifecycle reconciliation | `trr_backend/socials/control_plane/run_lifecycle.py` | legacy `reconcile_run_summaries` remains importable |
| dispatch runtime claims | `trr_backend/socials/control_plane/dispatch_runtime.py` | legacy `claim_next_queued_jobs`, `process_claimed_job`, `recover_and_dispatch_due_social_jobs` remain importable |

Guardrail: `tests/repositories/test_social_control_plane_imports.py` now asserts these owner relationships.

## Temporary Legacy Bridges

Direct legacy imports under `trr_backend/socials/control_plane/` are currently allowed only in these files:

| File | Planned phase |
| --- | --- |
| `analytics.py` | Phase 6 analytics/read-model extraction |
| `dispatch.py` | Phase 5 platform ingest orchestration extraction |
| `dispatch_runtime.py` | Phase 2 dispatch runtime extraction |
| `models.py` | Phase 2 shared model extraction |
| `queue_status.py` | Phase 2 queue status helper dependency cleanup |
| `recovery.py` | Phase 2 recovery/remediation extraction |
| `run_lifecycle.py` | Phase 2 run lifecycle extraction |
| `run_reads.py` | Phase 2 run read helper dependency cleanup |
| `runtime.py` | Phase 2 runtime/auth helper extraction |
| `shared_accounts.py` | Phase 3 account catalog/profile extraction |
| `shared_status_reads.py` | Phase 2 shared status helper dependency cleanup |
| `windowing.py` | Phase 6 analytics/windowing extraction |
| `worker_health.py` | Phase 2 worker health extraction |

Guardrail: `tests/repositories/test_social_control_plane_imports.py` now fails if this list changes without an explicit phase marker update.

## Phase 2 Candidate Helper Clusters

### Worker Health

Public surfaces:

- `update_worker_heartbeat`
- `mark_worker_stopped`
- `purge_inactive_workers`
- `get_worker_health`
- `get_worker_detail`
- `assert_worker_available_when_queue_enabled`

Observed direct helper dependencies from the legacy hub:

- `update_worker_heartbeat`: `_normalize_worker_stage`, `_normalize_worker_status`, `_worker_heartbeat_schema_ready`, `pg.fetch_one`, `json.dumps`
- `mark_worker_stopped`: calls `update_worker_heartbeat`
- `purge_inactive_workers`: `_resolve_positive_int_env`, `_worker_heartbeat_schema_ready`, `pg.execute_returning`, `pg.fetch_one`

Extraction note: move normalizers and schema-readiness checks together with worker health, or leave a temporary bridge marker until all worker-health functions move.

### Queue Status

Public surface:

- `get_queue_status`

Current canonical file:

- `trr_backend/socials/control_plane/queue_status.py`

Current issue:

- The canonical file still imports the legacy hub dynamically for cache state, relation checks, worker health, recovery helpers, and DB helpers.

Extraction note: Phase 2 should either move cache primitives and queue helper functions into `queue_status.py` or move them into a shared control-plane utility module used by `queue_status.py`.

### Dispatch Runtime

Public surfaces:

- `claim_next_queued_jobs`
- `process_claimed_job`
- `recover_and_dispatch_due_social_jobs`

Observed direct helper dependencies from the legacy hub:

- `claim_next_queued_jobs`: `_claim_next_jobs`, `_normalize_platform_name`, `_resolve_job_claim_batch_size`
- `process_claimed_job`: `_execute_claimed_job`
- `recover_and_dispatch_due_social_jobs`: `_modal_dispatch_limit`, `recover_stale_running_jobs`, `recover_stale_unclaimed_dispatched_jobs`, `dispatch_due_social_jobs`

Extraction note: `_execute_claimed_job` is likely a large boundary. Do not move it until Phase 2 has stable queue/run lifecycle ownership and Phase 5 platform ingest ownership is ready.

## Phase 3 Candidate Helper Clusters

### Catalog Launch and Run Progress

Public surfaces:

- `start_social_account_catalog_backfill`
- `begin_social_account_catalog_backfill_launch`
- `finalize_social_account_catalog_backfill_launch`
- `launch_social_account_catalog_backfill`
- `get_social_account_catalog_run_progress`

Observed direct helper dependencies from the legacy hub:

- `start_social_account_catalog_backfill`: `_assert_social_account_profile_exists`, `_build_social_account_catalog_launch_placeholder_config`, `_catalog_launch_initial_status`, `_reserve_social_account_catalog_launch`, `_record_social_account_catalog_launch_failure`, `_shared_account_catalog_requires_modal_executor`, `resolve_social_account_catalog_action_seed`, `ingest_shared_accounts`
- `get_social_account_catalog_run_progress`: `_load_social_account_catalog_run_row`, `_load_social_account_catalog_jobs`, `_build_run_progress_snapshot_payload`, `_build_catalog_run_progress_alerts`, `_summarize_run_progress_job_rows`, `_update_run_summary`, `_finalize_run_status`, recovery helpers, profile-total helpers, and shared-account progress helpers

Extraction note: split catalog launch and catalog progress into separate modules. Catalog progress depends on run lifecycle and queue recovery, so move it after Phase 2 control-plane helpers stabilize.

### Profile Reads

Public surfaces:

- `get_social_account_profile_summary`
- `get_social_account_profile_posts`
- `get_social_account_profile_comments`
- `get_social_account_profile_hashtags`
- `get_social_account_profile_collaborators_tags`

Observed direct helper dependencies from the legacy hub:

- `get_social_account_profile_summary`: normalization helpers, summary connection helpers, grouped-count helpers, entity aggregate builders, comments coverage helpers, detail-rollup helpers, and recent-run helpers
- `get_social_account_profile_posts`: profile existence checks, row fetchers, comments-only page fetchers, known-handle identity index, post item formatter, total-post helpers
- `get_social_account_profile_comments`: profile existence checks, profile connection helpers, shared catalog row fetch, comment row formatter, post detail discussion helpers

Extraction note: create `trr_backend/socials/account_catalog/profile_reads.py` and move read-only helpers with the public functions. Keep route payload contracts unchanged.

## Phase 4 Scrapling Ownership Boundary

Current Scrapling packages remain canonical:

- `trr_backend/socials/instagram/comments_scrapling/`
- `trr_backend/socials/instagram/posts_scrapling/`
- `trr_backend/socials/tiktok/posts_scrapling/`
- `trr_backend/socials/threads/posts_scrapling/`

Observed legacy imports still appear in lane job/session/persistence files. Those imports should be removed only when the canonical control-plane or account-catalog target exists.

Do not move fetchers, sessions, proxies, persistence adapters, retry classification, cookie bridge behavior, or paced request behavior into generic control-plane modules.

## Phase 6 Analytics Helper Clusters

Public surfaces:

- `get_analytics`
- `get_week_detail`
- `get_comments_coverage`
- `get_mirror_coverage`
- TikTok read models
- CSV/PDF builders

Observed direct helper dependencies from the legacy hub:

- `get_analytics`: week-window helpers, requested-platform normalization, row builders, sentiment context, driver builders, cache helpers, Reddit summary helpers, target-account helpers
- `get_week_detail`: platform detail handlers, coverage/status builders, week-run overlays, thread pool loading, sort helpers, media/comment coverage helpers
- `get_comments_coverage`: coverage window resolver, platform status builders, active job overlays, target-account helpers

Extraction note: leave these until mutation/runtime and account-catalog movement is stable. If `analytics.py` remains too large, create `trr_backend/socials/analytics/`.

## Caller and Patch-Path Inventory

High-volume legacy import callers remain in:

- `api/routers/socials.py`
- `scripts/socials/worker.py`
- `trr_backend/socials/profile_dashboard.py`
- `trr_backend/socials/control_plane/*`
- Scrapling lane job/session/persistence modules
- many repository and API tests

Migration rule:

- Router and script code should migrate to canonical imports only after the canonical owner exists and the legacy wrapper test passes.
- Tests should patch canonical import paths for canonical-owned surfaces.
- Keep a small legacy compatibility test suite for old import paths.

## Next Phase Readiness

Phase 2 runtime extraction is not ready while the dirty Instagram comments/cookie work remains unresolved.

Safe next steps before runtime movement:

1. Commit, stash, or explicitly assign the dirty Instagram comments/cookie files.
2. Run `pytest -q tests/repositories/test_social_control_plane_imports.py`.
3. Run `python -m compileall -q trr_backend/repositories/social_season_analytics.py trr_backend/socials`.
4. Start Phase 2 with worker-health and queue-status helper movement, not account catalog or analytics.
