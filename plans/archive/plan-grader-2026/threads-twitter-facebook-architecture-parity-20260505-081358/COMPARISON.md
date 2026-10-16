# Plan Comparison

## Inputs

- Source plan: `docs/codex/plans/2026-05-05-threads-twitter-facebook-architecture-parity.md`
- Revised plan: `.plan-grader/threads-twitter-facebook-architecture-parity-20260505-081358/REVISED_PLAN.md`

## Score Movement

| Plan | Score | Rating | Decision |
| --- | ---: | --- | --- |
| Source | 86.76 | Good plan; execute with minor tightening | Revise before parallel execution |
| Revised | 92.14 | Ready to execute | Execute with `orchestrate-subagents` |

## What Stayed The Same

- Same objective: improve Threads, Twitter/X, and Facebook social architecture parity.
- Same core file surfaces:
  - `trr_backend/socials/social_season_analytics_impl.py`
  - `trr_backend/socials/threads/posts_scrapling/*`
  - new platform `posts_catalog` Modules
  - `scripts/modal/verify_modal_readiness.py`
  - platform and shared tests
  - runbook and architecture docs
- Same non-goals:
  - no schema changes,
  - no route payload changes,
  - no new scrape stages,
  - no app contract changes,
  - no comments refactor,
  - no unsafe batch upsert.

## What Changed

| Area | Source Plan | Revised Plan |
| --- | --- | --- |
| Phase 0 tests | Names files and categories | Adds exact test node IDs and expected pre-fix failures |
| Remote auth | Names loaders and validators | Adds registry preference, safe structure flags, and CLI strict-readiness checks |
| Threads lane | Describes cancellation/degraded metadata work | Defines exact cancellation check points, error code, degraded summary, and skip-reason result |
| Twitter/X catalog | Extracts from monolith | Adds no-new-lane stop rules and protects CLI persistence behavior |
| Facebook catalog | Extracts from monolith | Adds no-new-lane stop rules and keeps comments/share/media out of scope |
| Threads catalog | Extracts shared catalog | Explicitly separates shared catalog from `posts_scrapling` lifecycle |
| Orchestration | Recommends workers | Defines disjoint write scopes and main-session integration ownership |
| Validation | Lists useful commands | Adds targeted node IDs and final validation grouping |
| Cleanup | Not explicit | Adds required archive and cleanup sections |

## Why The Revised Plan Is Safer

The current repo has a very dirty worktree and a large compatibility module. The revised plan reduces collision risk by making platform-local workers build Modules first while the main session owns the wrapper integration. It also prevents well-intentioned lane expansion for Twitter/X and Facebook, which would exceed the requested architecture parity scope.

## Execution Recommendation

Execute the revised plan, not the source plan. The source plan should remain as historical input only.
