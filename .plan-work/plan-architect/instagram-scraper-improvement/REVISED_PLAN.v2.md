# REVISED_PLAN.v2.md

## Beneficial Capabilities For This Plan

| Capability | Type | Direct Use | Invocation Path | Validation Contribution | Documentation Source |
| --- | --- | --- | --- | --- | --- |
| Plan Architect | Plugin | Grade, tighten, and hand off the implementation plan. | `[@plan-grader](plugin://plan-grader@local-plugins)` compatibility alias. | Produces v2 artifact set and readiness score. | Plan Architect plugin contracts. |
| Tool Finder | Plugin helper | Discover implementation candidates and reject irrelevant packages. | `scripts/run_tool_finder_for_plan.py ... --artifact-suffix v2`. | Confirms repo-local seams are useful; records noisy candidates as rejected. | `TOOLS.v2.md`, `TOOL_FINDER_RESULTS.v2.json`. |
| TRR Backend tests | Local commands | Validate benchmark, queue, completion, comments, media, and Modal seams. | Targeted `pytest` commands per phase. | Proves behavior without broad unrelated churn. | Existing tests under `tests/`. |
| Modal readiness tooling | Local/Modal scripts | Verify remote scraper/runtime follow-through after code changes. | `scripts/modal/verify_modal_readiness.py --json` and Instagram auth probes. | Prevents local-only completion claims. | `scripts/modal/verify_modal_readiness.py`. |
| Repo-local DB helpers | Local scripts | Apply additive SQL and inspect live schema when needed. | `scripts/db/run_sql.sh` and targeted schema readbacks. | Keeps Supabase changes explicit and reversible. | Backend runbooks and migrations. |
| GitHub issue tracker | Optional external traceability | Link implementation issues only after live issue verification. | GitHub connector or `gh` during execution, if needed. | Traceability only; not readiness proof for this plan. | Unverified in this rerun. |

## Goal

Implement the Instagram scraper improvement project from `docs/codex/prds/instagram-scraper-improvement.md` in ordered, agent-ready slices. The delivered system must produce Complete Instagram Post Snapshots for bounded Bravo account/date-window backfills, measure real account-level speed, track missing parts explicitly, retry only incomplete work, adapt speed to Supabase and Instagram/proxy health, and validate Modal production readiness for runtime changes.

## Non-Goals

- Do not replace TRR-native scraping with Apify or a third-party Instagram client.
- Do not adopt generic PyPI/Homebrew packages returned by Tool Finder unless a later implementation proves a narrow, repo-compatible need.
- Do not persist logged-in viewer state as product data.
- Do not treat retry exhaustion as source-unavailable evidence.
- Do not launch unbounded live Instagram runs.
- Do not change TRR-APP behavior until backend API/reporting contracts require it.

## Tool Finder Notes

Tool Finder completed for v2 and returned 34 candidates. Useful candidates are the local TRR instructions, tests, scripts, and runbooks. Generic packages such as `instagram`, `scraper`, `complete`, `snapshot`, `pcl`, `abcl`, and `abpoa` are rejected for this plan because they do not improve the repo-native Instagram scraper contract and would create scope drift.

## Reality Verification

| Claim | Evidence | Status | Contradiction Check | Plan Consequence |
| --- | --- | --- | --- | --- |
| PRD exists and defines complete snapshot, speed, safety, benchmarks, retry, source-unavailable, Modal, and non-goals. | `docs/codex/prds/instagram-scraper-improvement.md:1-128`. | verified_source | PRD explicitly rejects Apify replacement, retry exhaustion as source truth, and unbounded full-history scraping. | Plan traces to current PRD. |
| Domain terms and ADR exist. | `CONTEXT.md:7-45`; `docs/adr/0001-adaptive-instagram-scrape-control-plane.md:1-12`. | verified_source | ADR supports shared pressure decisions with lane-specific enforcement. | Use glossary and shared control-plane architecture. |
| Posts benchmark helper is payload-only and side-effect-free. | `scripts/socials/instagram/benchmark_posts_backfill.py:1-7`, `:68-112`. | verified_source | It emits `live_scrape_executed: False`, zero metrics, and placeholder timing fields. | Phase 1 extends this seam into account/date-window reporting. |
| Comments benchmark helper supports fixture and guarded live modes. | `scripts/socials/instagram/benchmark_comments_shards.py:1-7`, `:43-88`, `:187-225`, `:451-462`. | verified_source | Live mode refuses without `--confirm-live` and `--active-job-preflight`. | Reuse this helper rather than replacing it. |
| Backfill health aggregates auth, queue, worker, cooldown, bandwidth, and progress signals. | `trr_backend/socials/control_plane/backfill_health.py:1-35`, `:319-405`. | verified_source | Current implementation degrades fail-open and reads queue summary. | Use as pressure evidence input. |
| Queue status exposes stale jobs, dispatch blocks, runs summary, silent-drop alerts, and running jobs. | `tests/repositories/test_social_queue_status.py:544-640`, `:666-671`, `:958-970`, `:1022-1038`. | verified_source | Tests assert the payloads required by the control-plane pressure model. | Use queue status as pressure input. |
| Comment persistence stores author/profile metadata and comment media state conditionally. | `trr_backend/socials/instagram/comments_scrapling/persistence.py:10-27`, `:30-55`, `:648-755`. | verified_source | Current code preserves metadata and gates partial schema columns safely. | Extend for completion/reporting rather than re-parsing from scratch. |
| Media queue guard exists for stale media claims. | `scripts/socials/media_queue_guard.py:1-109`. | verified_source | It blocks when stale `media_mirror` or `comment_media_mirror` jobs are running unless override is explicit. | Reuse pattern for media lane safety. |
| Modal readiness supports JSON output and Instagram posts/comments auth probes. | `scripts/modal/verify_modal_readiness.py:119-185`, `:729-785`, `:1150-1183`. | verified_source | It reports completion evidence and deployed probe readiness. | Runtime phases require Modal validation/deploy gates. |
| Required validation paths exist. | Path existence command checked phase test files and `scripts/db/run_sql.sh`. | verified_runtime | All checked paths existed in current worktree. | Validation commands are executable targets, not speculative paths. |
| Tool Finder completed for v2. | `TOOLS.v2.md`; `TOOL_FINDER_RESULTS.v2.json`; command returned status `completed`. | verified_runtime | External package results are noisy and rejected. | Use local repo seams; do not adopt generic packages. |
| GitHub issue traceability is not proven in this rerun. | Supplied plan names `therealityreport/trr-backend#149`; no live GitHub query was performed. | unverified_inference | Plan no longer depends on that issue for readiness. | Treat GitHub issue linkage as optional traceability until verified live. |
| Exact live thresholds are not yet proven. | No threshold table was found in inspected files during the review. | unverified_inference | Plan does not hard-code aggressive permanent defaults. | Use conservative defaults and benchmark-scoped ramping. |
| Worktree is dirty with unrelated backend changes. | `git status --short --branch` in `TRR-Backend` showed many modified/untracked files. | verified_runtime | Dirty files predate this planning artifact update and are not reverted. | Implementation handoff must preserve unrelated state. |

## Phase 1: Baseline Benchmark And Gap Report

Objective: create the first executable benchmark before optimizing.

Tasks:

1. Extend the posts benchmark helper into an account/date-window report command that accepts account, bounded date window, source scope, optional run id, and output path.
2. Combine posts benchmark output, comments benchmark output, backfill progress, queue status, and backfill health into one report shape.
3. Include account runtime, p95 post detail/comment timing, phase durations, completeness gaps, retry volume, Supabase pressure, Instagram/proxy risk, active lane budgets, and source-unavailable counts.
4. Keep fixture mode default and require explicit confirmation plus active-job preflight for live read-only mode.
5. Produce Markdown and JSON reports under `.logs/instagram-benchmarks/` with `latest.md` and `latest.json` aliases.

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

If SQL changes are needed, also run migration lint and a schema/readback check through `scripts/db/run_sql.sh`.

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
5. Lanes record budget state and cause in job metadata.

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
2. Update runbooks for benchmark usage, live-mode guardrails, budget interpretation, retry recovery, and rejected third-party package scope.
3. Run targeted backend validation.
4. For any scraper/job/runtime/Modal-secret code changes, deploy or update Modal and run readiness/auth probes.
5. Only touch TRR-APP if backend API changes require UI follow-through; otherwise document app build as not applicable.
6. If GitHub issue tracking is used, verify the live issue before using it as coordination proof.

Validation:

```bash
python scripts/modal/verify_modal_readiness.py --json
python scripts/modal/verify_modal_readiness.py --probe-instagram-posts-auth bravotv --json
python scripts/modal/verify_modal_readiness.py --probe-instagram-comments-auth bravotv --json
```

Use strict comments auth only when the specific implementation issue requires authenticated comments proof.

## Execution Notes

- Use Context7 only when implementation changes external library/API behavior, such as Scrapling, Patchright, Modal CLI/SDK, or Supabase usage.
- Preserve unrelated dirty-tree changes.
- Keep backend/API validation, SQL ownership, app build relevance, and Modal follow-through separate in every completion note.

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
