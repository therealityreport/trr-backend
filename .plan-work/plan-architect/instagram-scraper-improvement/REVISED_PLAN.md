# REVISED_PLAN.md

## Beneficial Capabilities For This Plan

| Capability | Type | Direct Use | Invocation Path | Validation Contribution | Documentation Source |
| --- | --- | --- | --- | --- | --- |
| Plan Architect | Plugin | Build, audit, grade, revise, and hand off the implementation plan. | `[@plan-grader](plugin://plan-grader@local-plugins)` compatibility alias. | Produces full artifact set and readiness score. | Plan Architect plugin contracts. |
| Tool Finder | Plugin helper | Find useful local/external tools and reject irrelevant candidates. | `scripts/run_tool_finder_for_plan.py`. | Confirms local repo seams are the useful tools. | `TOOLS.md`, `TOOL_FINDER_RESULTS.json`. |
| TRR Backend tests | Local commands | Validate benchmark, queue, completion, comments, media, and Modal seams. | `pytest ...` targeted commands per phase. | Proves behavior without broad unrelated churn. | Existing tests under `tests/`. |
| Modal readiness tooling | Local/Modal scripts | Verify remote scraper/runtime follow-through after code changes. | `scripts/modal/verify_modal_readiness.py --json` and relevant probes. | Prevents local-only completion claims. | `scripts/modal/verify_modal_readiness.py`. |
| Repo-local DB helpers | Local scripts | Apply additive SQL and inspect live schema when needed. | `scripts/db/run_sql.sh`, schema docs, targeted SQL checks. | Keeps Supabase changes explicit and reversible. | Backend runbooks and migrations. |
| GitHub issue tracker | External issue tracker | PRD already published and labeled for downstream issue split. | `therealityreport/trr-backend#149`. | Keeps implementation work traceable. | GitHub issue. |

## Goal

Implement the Instagram scraper improvement project from `docs/codex/prds/instagram-scraper-improvement.md` in ordered, agent-ready slices. The delivered system must produce Complete Instagram Post Snapshots for bounded Bravo account/date-window backfills, measure real account-level speed, track missing parts explicitly, retry only incomplete work, adapt speed to Supabase and Instagram/proxy health, and validate Modal production readiness for runtime changes.

## Non-Goals

- Do not replace TRR-native scraping with Apify or a third-party Instagram client.
- Do not persist logged-in viewer state as product data.
- Do not treat retry exhaustion as source-unavailable evidence.
- Do not launch unbounded live Instagram runs.
- Do not change TRR-APP behavior until backend API/reporting contracts require it.

## Reality Verification

| Claim | Evidence | Status | Contradiction Check | Plan Consequence |
| --- | --- | --- | --- | --- |
| PRD exists and defines the project. | `docs/codex/prds/instagram-scraper-improvement.md`. | verified_source | File read in current turn. | Plan traces to PRD. |
| Domain terms and ADR exist. | `CONTEXT.md`, `docs/adr/0001-adaptive-instagram-scrape-control-plane.md`. | verified_source | Current files read. | Use glossary and shared control-plane architecture. |
| Posts benchmark helper exists but is payload-only. | `scripts/socials/instagram/benchmark_posts_backfill.py` shows `live_scrape_executed: False` and placeholder metrics. | verified_source | No implementation of account runtime aggregation in file. | Phase 1 extends this seam. |
| Comments benchmark helper supports fixture and guarded live modes. | `scripts/socials/instagram/benchmark_comments_shards.py` has fixture profiles, p95 timing, live guard args. | verified_source | Current file read. | Reuse rather than replace. |
| Backfill health already aggregates auth, queue, worker, cooldown, and proxy bandwidth signals. | `trr_backend/socials/control_plane/backfill_health.py` docstring and functions. | verified_source | Current source read. | Use as pressure evidence input. |
| Queue status exposes stale jobs, dispatch blocks, running jobs, and recent failures. | `tests/repositories/test_social_queue_status.py` grep output. | verified_source | Current tests show expected payloads. | Use queue status as pressure input. |
| Comment persistence stores author/profile and comment media state. | `trr_backend/socials/instagram/comments_scrapling/persistence.py` fields and payload logic. | verified_source | Current source read. | Extend for completion/reporting rather than re-parse from scratch. |
| Media queue guard exists for stale media claims. | `scripts/socials/media_queue_guard.py`. | verified_source | Current source read. | Reuse pattern for media lane safety. |
| Posts and comments runbooks forbid Apify replacement. | `docs/workspace/instagram-posts-scrapling.md`; `docs/social/instagram-data-contract.md`. | verified_source | Current docs read. | Reject external scraper replacement. |
| Modal follow-through is required for scraper/runtime code. | Root and backend AGENTS/rules plus Modal readiness script. | verified_source | Current instructions read. | Every runtime phase includes Modal validation/deploy gate. |
| Exact live thresholds are not yet proven. | No current threshold table found in inspected files. | unverified_inference | Must not hard-code aggressive defaults from PRD alone. | Use conservative defaults and benchmark-scoped ramping. |
| Worktree is dirty with unrelated changes. | `git status --short --branch` showed many modified/untracked backend files. | verified_runtime | Current command output. | Implementation handoff must preserve unrelated state. |

## Phase 1: Baseline Benchmark And Gap Report

Objective: create the first executable benchmark before optimizing.

Tasks:

1. Extend the posts benchmark helper into an account/date-window report command that accepts account, bounded date window, source scope, optional run id, and output path.
2. Combine posts benchmark output, comments benchmark output, backfill progress, queue status, and backfill health into one report shape.
3. Include account runtime, p95 post detail/comment timing, phase durations, completeness gaps, retry volume, Supabase pressure, Instagram/proxy risk, active lane budgets, and source-unavailable counts.
4. Keep fixture mode default and require explicit confirmation plus active-job preflight for live read-only mode.
5. Produce a Markdown and JSON report under `.logs/instagram-benchmarks/` with `latest.md` and `latest.json` aliases.

Validation:

```bash
pytest -q tests/scripts/test_benchmark_instagram_catalog_full_history.py \
  tests/scripts/test_benchmark_instagram_comments_shards.py \
  tests/repositories/test_social_queue_status.py
python -m py_compile scripts/socials/instagram/benchmark_posts_backfill.py \
  scripts/socials/instagram/benchmark_comments_shards.py
```

## Phase 2: Completion And Retry Target Model

Objective: make partial success explicit and prevent false complete status.

Tasks:

1. Define snapshot parts: post detail, canonical post row, media assets, hosted media, comments, replies, comment media, author avatar, collaborators/tags/location/music/ad flags where source data exists.
2. Add an additive persistence model using existing job/run metadata if sufficient; otherwise add nullable tables/columns for snapshot part state and retry targets.
3. Record `captured`, `retryable`, `source_unavailable`, `blocked`, and `deferred` states with reason codes and evidence.
4. Preserve valid data immediately and create targeted retry records only for missing parts.
5. Require Source-Unavailable Evidence for permanent unavailable state.

Validation:

```bash
pytest -q tests/socials/instagram/comments_scrapling/test_persistence.py \
  tests/socials/instagram/comments_scrapling/test_missing_comment_gap_sql.py \
  tests/scripts/test_backfill_instagram_metadata_and_media.py \
  tests/scripts/test_media_mirror_recovery.py
```

If SQL changes are needed, also run migration lint and a schema/readback check through the repo DB helper.

## Phase 3: Adaptive Scrape Control Plane

Objective: centralize budget decisions and preserve lane-specific enforcement.

Tasks:

1. Add a control-plane budget module that reads current pressure from backfill health, queue status, active cooldowns, recent failures, write failures, and optional benchmark-scoped overrides.
2. Publish budgets as `normal`, `reduced`, `paused`, or `identity_blocked`.
3. Enforce precedence: blocked identity, proxy cooldown, account-lane pause, global lane budget, default.
4. Persist budget state/evidence durably and cache read decisions in memory with a short TTL.
5. Start with conservative config defaults for unknown thresholds.

Validation:

```bash
pytest -q tests/repositories/test_social_queue_status.py \
  tests/repositories/test_social_control_plane_worker_health.py \
  tests/scripts/test_social_control_plane_pressure_snapshot.py
```

## Phase 4: Lane Enforcement

Objective: make each lane consume budgets without duplicating pressure policy.

Tasks:

1. Comments lane reduces per-post concurrency, pagination aggressiveness, and retry refill under `reduced`; pauses new work under `paused`; stops identity under `identity_blocked`.
2. Posts lane reduces page/detail fetch concurrency and doc-id attempts under `reduced`; pauses new requests under `paused`.
3. Media mirror lane reduces download/upload concurrency and respects stale media guard state.
4. DB write path reduces batch size/concurrency when Supabase pressure rises.
5. Lanes must record budget state and cause in job metadata.

Validation:

```bash
pytest -q tests/socials/instagram/comments_scrapling/test_job_runner_concurrency.py \
  tests/socials/instagram/comments_scrapling/test_worker_cap_ramp.py \
  tests/socials/instagram/posts_scrapling/test_job_runner.py \
  tests/scripts/test_media_queue_guard.py
```

## Phase 5: Mega-Post Sharding And Cursor Recovery

Objective: keep large posts from stalling account runs.

Tasks:

1. Detect mega-post candidates from expected comments, observed runtime, retry stalls, and pagination deadlines.
2. Split mega-posts into one-post jobs with saved top-level and reply cursors.
3. Ensure retry jobs resume from saved checkpoints and ignore terminal repeated-cursor states.
4. Keep ordinary posts batched separately.

Validation:

```bash
pytest -q tests/socials/instagram/comments_scrapling/test_pagination_cursor_swap.py \
  tests/socials/instagram/comments_scrapling/test_job_runner_reply_only.py \
  tests/scripts/test_enqueue_comments_audit_cursor_retries.py
```

## Phase 6: Hosted Media And Comment Media Completion

Objective: make hosted media part of snapshot completion.

Tasks:

1. Treat source URLs as partial until hosted media/avatar/comment media mirror is complete or source-unavailable.
2. Queue comment media mirroring after text/reply capture.
3. Surface avatar and comment media gaps in completion reports.
4. Keep media mirror retries separate from comment text/reply retries.

Validation:

```bash
pytest -q tests/scripts/test_media_mirror_recovery.py \
  tests/scripts/test_one_post_media_mirror.py \
  tests/scripts/test_retire_duplicate_instagram_comment_media_mirror_jobs.py \
  tests/scripts/test_retire_duplicate_instagram_media_mirror_jobs.py
```

## Phase 7: Operator Reporting, API Surface, And Modal Follow-Through

Objective: make the new behavior visible and production-ready.

Tasks:

1. Expose completion, retry-target, source-unavailable, and budget state through existing progress/backfill-health/API surfaces.
2. Update runbooks for benchmark usage, live-mode guardrails, budget interpretation, and retry recovery.
3. Run targeted backend validation.
4. For any scraper/job/runtime/Modal-secret code changes, deploy or update Modal and run readiness/auth probes.
5. Only touch TRR-APP if backend API changes require UI follow-through; otherwise document app build as not applicable.

Validation:

```bash
python scripts/modal/verify_modal_readiness.py --json
python scripts/modal/verify_modal_readiness.py --probe-instagram-posts-auth bravotv --json
python scripts/modal/verify_modal_readiness.py --probe-instagram-comments-auth bravotv --json
```

Use strict comments auth only when the specific implementation issue requires authenticated comments proof.

## Rollback And Safety

- New schema must be additive and nullable.
- New defaults must start conservative.
- Benchmark-scoped ramping must not change permanent defaults.
- Live runs require explicit confirmation and active-job preflight.
- Modal deploys must be reported separately from local validation.
- Preserve unrelated dirty-tree changes.

## Cleanup After Implementation

- Mark this plan superseded/completed after all implementation issues land and validation passes.
- Delete only temporary benchmark outputs that are not intended as evidence.
- Keep durable PRD, ADR, glossary, and runbook updates.
