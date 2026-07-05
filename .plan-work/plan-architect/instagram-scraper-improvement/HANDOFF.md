# HANDOFF.md

## Approved Plan

Use `REVISED_PLAN.md` in this folder as the approved implementation plan for the Instagram scraper improvement project.

## Execution Mode

`orchestrate-subagents`

The work is backend-first and separable by phase, but phases must be serialized where later work depends on prior state contracts.

## Ownership Scopes

1. **Benchmark Scope**: Extend account/date-window benchmark and gap report. Own scripts, report shape, fixture tests, guarded live-read mode.
2. **Completion Scope**: Add/extend snapshot-part completion and retry target state. Own persistence model, source-unavailable reasons, completion reporting.
3. **Control-Plane Scope**: Build shared budget decision service. Own pressure inputs, precedence, cache/persistence, tests.
4. **Lane Enforcement Scope**: Wire comments/posts/media/DB write lanes to budgets. Own lane behavior and job metadata evidence.
5. **Mega-Post Scope**: Add one-post sharding and cursor/checkpoint retry behavior for high-comment posts.
6. **Media Completion Scope**: Enforce hosted media/avatar/comment media completion or source-unavailable status.
7. **Operations Scope**: Update API/progress/runbooks and perform Modal validation/deploy follow-through for runtime changes.

## Ordering

1. Benchmark Scope.
2. Completion Scope.
3. Control-Plane Scope.
4. Lane Enforcement Scope.
5. Mega-Post Scope.
6. Media Completion Scope.
7. Operations Scope and final integration.

Do not tune permanent runtime defaults before benchmark evidence exists.

## Branch And Worktree Policy

- Work on the current branch unless the user explicitly asks for a new branch.
- Run `git status --short --branch` in both TRR root and `TRR-Backend` before edits.
- Preserve unrelated dirty-tree changes.
- Do not stage or commit unless explicitly asked.

## Validation Matrix

| Scope | Required Validation |
| --- | --- |
| Benchmark | `pytest -q tests/scripts/test_benchmark_instagram_catalog_full_history.py tests/scripts/test_benchmark_instagram_comments_shards.py tests/repositories/test_social_queue_status.py` |
| Completion | Comments persistence, missing-gap, metadata/media recovery tests; SQL readback if schema changes. |
| Control plane | Queue status, worker health, pressure snapshot tests. |
| Lane enforcement | Comments concurrency/worker-cap tests, posts job-runner tests, media queue guard tests. |
| Mega-post | Cursor swap, reply-only, audit cursor retry tests. |
| Media completion | Media mirror recovery, one-post mirror, duplicate-retirement tests. |
| Modal/runtime | `python scripts/modal/verify_modal_readiness.py --json` plus relevant Instagram auth probes after deploy. |

## Conflict Controls

- Treat existing untracked scripts/tests as user or prior-agent work unless they are directly in scope.
- If a file already has unrelated edits, read it and patch around them.
- For SQL ownership, use backend migrations and direct-SQL ledger practices.
- For live Instagram runs, require explicit confirmation and active-job preflight.

## Final Integration

The lead agent owns final integration, validation synthesis, Modal follow-through status, SQL status, and TRR-APP applicability. TRR-APP build is not applicable unless backend API/app contract changes require app follow-through.

readyForHandoff: true
