# REVISED_PLAN.v3.md

## Beneficial Capabilities For This Plan

| Capability | Type | Direct Use | Invocation Path | Validation Contribution | Documentation Source |
| --- | --- | --- | --- | --- | --- |
| Plan Architect revise-plan | Plugin skill | Rewrite the current plan to make the requested implementation items explicit. | `plan-grader:revise-plan`. | Produces v3 revised plan, scorecard, validation, result pointer, and implementation handoff. | Plan Architect `revise-plan` skill contracts. |
| Subagents | Multi-agent execution | Implement disjoint backend slices for budget control, completion/retry state, and media gates. | `orchestrate-subagents` via worker subagents. | Keeps ownership clear while preserving current dirty-tree state. | TRR AGENTS subagent rules. |
| TRR Backend tests | Local commands | Validate control-plane, completion, comments persistence, media gates, and queue seams. | Targeted `pytest` and `py_compile` commands. | Confirms behavior without broad unrelated churn. | Existing tests under `tests/`. |
| Modal readiness tooling | Local/Modal scripts | Validate deployed runtime only when scraper/job/runtime code changes are sent to Modal. | `scripts/modal/verify_modal_readiness.py --json` and Instagram probes. | Separates local validation from production readiness. | TRR Modal readiness script. |
| Repo-local DB helpers | Local scripts | Apply/read back additive SQL only if schema ownership is required. | `scripts/db/run_sql.sh`. | Keeps Supabase changes explicit and reversible. | Backend DB scripts/runbooks. |

## Goal

Implement the Instagram scraper improvement work in three immediate backend slices, after the existing benchmark groundwork:

3. Create the adaptive control-plane budget module.
4. Implement snapshot completion and retry target tracking.
5. Add hosted media and comment media completion gates.

The delivered backend must expose budget decisions, completion state, retryable gaps, source-unavailable evidence, and media completion gates through repo-native surfaces without adopting third-party Instagram scraper packages.

## Non-Goals

- Do not replace TRR-native Instagram scraping with Apify or a generic Instagram package.
- Do not run unbounded live Instagram work.
- Do not mark retry exhaustion as source-unavailable evidence.
- Do not persist viewer-specific logged-in state as product data.
- Do not touch TRR-APP unless backend API/reporting changes require UI follow-through.

## Reality Verification

| Claim | Evidence | Status | Contradiction Check | Plan Consequence |
| --- | --- | --- | --- | --- |
| Current v2 plan already names the control-plane, completion, and media phases. | `REVISED_PLAN.v2.md` Phase 2, Phase 3, Phase 6. | verified_source | User requested making these implementation items explicit, not replacing the whole plan. | v3 promotes them to immediate numbered slices 3, 4, and 5. |
| Backfill health is the right pressure input seam. | `trr_backend/socials/control_plane/backfill_health.py` aggregates progress, auth cooldowns, worker/auth health, queue depth, and bandwidth. | verified_source | No need for a new independent health reader. | Budget module should consume this seam. |
| Queue status is the right queue/dispatch/running-job pressure seam. | `tests/repositories/test_social_queue_status.py` covers stale jobs, dispatch-blocked payloads, runs summary, silent-drop alerts, and running jobs. | verified_source | Avoid duplicating queue SQL. | Budget module should consume `get_queue_status`. |
| Comment persistence already has author/comment-media fields and column-existence gates. | `trr_backend/socials/instagram/comments_scrapling/persistence.py` conditionally writes author metadata, media URLs, hosted media URLs, and media mirror state. | verified_source | Completion work should extend state/reporting rather than re-parse comments. | Completion/retry and media gates can build on current persistence payloads. |
| Media stale claim guard exists. | `scripts/socials/media_queue_guard.py` blocks stale `media_mirror` and `comment_media_mirror` claims. | verified_source | Media completion gates should respect this pattern. | Reuse stale media evidence as a gate input. |
| Exact production thresholds remain unproven. | No threshold table was verified during this revise-plan pass. | unverified_inference | Plan uses conservative defaults and benchmark-scoped overrides. | Budget defaults must start cautious and observable. |
| Worktree is dirty. | `git status --short --branch` shows many unrelated modified/untracked files. | verified_runtime | Do not revert unrelated changes. | Subagents must use disjoint ownership and preserve existing edits. |

## Immediate Implementation Sequence

### 3. Create The Adaptive Control-Plane Budget Module

Owner: Budget subagent.

Tasks:

1. Add a repo-native budget module under the existing social control-plane package.
2. Define lane budget states: `normal`, `reduced`, `paused`, and `identity_blocked`.
3. Read pressure from backfill health, queue status, active cooldowns, recent failures, stale/running jobs, write-failure indicators, and optional benchmark-scoped overrides.
4. Enforce precedence: blocked identity, proxy cooldown, account-lane pause, global lane budget, default.
5. Return a structured decision with state, lane, account, reasons, evidence, limits, generated timestamp, and TTL.
6. Start with conservative defaults and pure/read-only behavior unless a later execution step adds persistence.
7. Acceptance criterion for the follow-through issue: Instagram backfill dispatch must consult the current lane budget before creating or starting catalog jobs. Reduced budgets cap worker counts. Paused and identity-blocked budgets should prevent new catalog dispatch and persist `blocked_budget` with the full `budget_decision` in run config/job metadata. Retryable blocked runs must remain resumable once the budget recovers.
8. Caveat before implementing `blocked_budget` as a concrete status: inspect existing DB status constraints/enums first. If the schema does not allow the status yet, either add the migration/constraint update or store `blocked_budget` as metadata/error code while preserving the current valid run/job status.

Validation:

```bash
pytest -q tests/repositories/test_social_control_plane_worker_health.py \
  tests/repositories/test_social_queue_status.py \
  tests/scripts/test_social_control_plane_pressure_snapshot.py
python -m py_compile trr_backend/socials/control_plane/*.py
```

### 4. Implement Snapshot Completion And Retry Target Tracking

Owner: Completion subagent.

Tasks:

1. Define snapshot part names for post detail, canonical post row, media assets, hosted media, comments, replies, comment media, author avatar, collaborators, tags, location, music, and ad flags.
2. Add a model/helper that converts captured evidence and missing parts into completion state.
3. Support `captured`, `retryable`, `source_unavailable`, `blocked`, and `deferred` states.
4. Require source-unavailable evidence before permanent unavailable state.
5. Emit targeted retry records for missing parts without redoing captured parts.
6. Prefer metadata-backed tracking first; add schema only if current structures cannot represent the contract.

Validation:

```bash
pytest -q tests/socials/instagram/comments_scrapling/test_persistence.py \
  tests/socials/instagram/comments_scrapling/test_missing_comment_gap_sql.py \
  tests/scripts/test_backfill_instagram_metadata_and_media.py
python -m py_compile trr_backend/socials/instagram/*.py
```

If SQL changes are needed, use additive nullable migrations and a repo DB helper readback.

### 5. Add Hosted Media And Comment Media Completion Gates

Owner: Media gate subagent.

Tasks:

1. Treat source URLs as partial until hosted media, avatar mirrors, and comment media mirrors are complete or source-unavailable.
2. Add gate helpers that classify hosted media, author avatar, and comment media completion for a snapshot.
3. Keep media mirror retry targets separate from comment text/reply retry targets.
4. Respect stale media queue evidence and avoid claiming completion while stale media claims exist.
5. Surface avatar/comment-media gaps in benchmark or progress-compatible payloads.

Validation:

```bash
pytest -q tests/scripts/test_media_mirror_recovery.py \
  tests/scripts/test_one_post_media_mirror.py \
  tests/scripts/test_media_queue_guard.py \
  tests/scripts/test_retire_duplicate_instagram_comment_media_mirror_jobs.py \
  tests/scripts/test_retire_duplicate_instagram_media_mirror_jobs.py
python -m py_compile scripts/socials/media_queue_guard.py
```

## Subagent Execution Rules

- Use worker subagents with disjoint write ownership.
- Every worker must start from current files and must not read saved notes, prior sessions, handoffs, or memories.
- Workers must not revert unrelated dirty-tree changes.
- Workers must list changed files and validation commands.
- The lead integrates results, resolves contract overlap, and runs final validation.

## Completion Rules

- Backend/API validation must be reported.
- SQL ledger/readback must be reported if SQL ownership changes.
- TRR-APP build is not applicable unless backend API changes require app follow-through.
- Modal follow-through is required for scraper/job/runtime/Modal secret-prep changes unless explicitly scoped local-only.
- Browser verification is only needed if an app/UI surface is touched.

## Cleanup After Implementation

- Mark v3 as superseded/completed only after implementation and validation pass.
- Keep durable PRD, ADR, glossary, runbook, and evidence artifacts.
- Delete only temporary benchmark output that is not retained as evidence.
