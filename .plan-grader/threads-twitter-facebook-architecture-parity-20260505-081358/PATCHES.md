# Revision Patches

Source plan: `docs/codex/plans/2026-05-05-threads-twitter-facebook-architecture-parity.md`
Revised plan: `.plan-grader/threads-twitter-facebook-architecture-parity-20260505-081358/REVISED_PLAN.md`

## Patch 1: Make Phase 0 Executable

Problem:

The source plan correctly says to write tests first, but several test tasks are broad. In this repo, broad social analytics tests can create noise and make parallel work harder to evaluate.

Revision:

Added exact test names for Threads cancellation/degraded behavior, remote auth safe structure flags, Modal readiness CLI parsing, and wrapper delegation.

Expected effect:

Workers can add failing tests first and run focused node IDs before editing production code.

## Patch 2: Tighten No-New-Lane Boundaries

Problem:

The source plan says Twitter/X and Facebook are shared catalog extraction work, but the repo has nearby worker-lane patterns that could tempt an executor into adding new stages or dispatch paths.

Revision:

Added stop rules forbidding new Twitter/X and Facebook claimed-job lanes, dispatch cases, stages, routes, and route payload changes.

Expected effect:

The implementation stays architecture-focused and avoids accidental app/backend contract expansion.

## Patch 3: Make Remote Auth Probe Safe And Registry-Aligned

Problem:

The source plan names the correct loaders and validators, but does not specify how to avoid duplicated branch logic or secret leakage in probe payloads.

Revision:

Required use of existing cookie registry helpers where possible and added explicit safe structure flags for Twitter/X, Facebook, and Threads.

Expected effect:

Readiness parity is implemented without raw secret exposure and without drifting away from existing cookie validation contracts.

## Patch 4: Split Threads Lanes Explicitly

Problem:

Threads has both a claimed-job Scrapling lane and shared catalog behavior. These are easy to confuse during refactor work.

Revision:

Assigned `threads/posts_scrapling` hardening and `threads/posts_catalog` extraction to different work scopes, and added a stop rule against merging their Interfaces.

Expected effect:

Lifecycle concerns and shared catalog concerns stay independently testable.

## Patch 5: Define Subagent Ownership

Problem:

The source plan recommends subagents but leaves shared-file ownership too loose.

Revision:

Added an orchestration plan with Worker A/B/C/D scopes and main-session ownership of compatibility wrappers, readiness CLI, import-cycle tests, docs, fixtures, and final validation.

Expected effect:

Parallel implementation can move faster without edit collisions in `social_season_analytics_impl.py`.

## Patch 6: Preserve Batch-Upsert Safety Gate

Problem:

The source plan correctly defers batch upsert, but this needs to remain visible during platform extraction because catalog persistence is a likely refactor temptation.

Revision:

Added batch-upsert stop rules and a docs task to record deferred status unless equivalence tests prove safety.

Expected effect:

No silent persistence-contract drift.

## Patch 7: Add Queue And Fixture Evidence

Problem:

Operator-facing work should leave evidence that queue status and metadata remain truthful.

Revision:

Added fixture tasks for cancellation/degraded/readiness failures and queue status tests for new or preserved error codes.

Expected effect:

The implementation can be reviewed against persisted/operator-facing metadata, not just unit-test mocks.
