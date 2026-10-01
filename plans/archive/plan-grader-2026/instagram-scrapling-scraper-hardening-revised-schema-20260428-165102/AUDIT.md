# Audit

Source plan: `.plan-grader/instagram-scrapling-scraper-hardening-20260428-164020/REVISED_PLAN.md`

Source suggestions: `.plan-grader/instagram-scrapling-scraper-hardening-20260428-164020/SUGGESTIONS.md`

## Verdict

`APPROVED_WITH_REVISIONS`

The prior revised plan fixed the original correctness blockers, but it still used the older execution-plan shape and referenced Superpowers execution framing. The user requested the new plan schema, all suggestions incorporated, and `orchestrate-subagents` as the execution path. The new `REVISED_PLAN.md` now follows the user-global `write-plan` schema, integrates every numbered suggestion as a required task, and makes `orchestrate-subagents` the explicit handoff.

## Current-State Fit

Confirmed against the repo during revision:

- Current branch is `chore/backend-batch-2026-04-28`, not `main`.
- `orchestrate-subagents` requires `main` unless the user explicitly approves continuing on another branch, so the revised plan includes a hard preflight stop rule.
- Current dirty state is untracked planning artifacts only: `.plan-grader/` and `docs/superpowers/plans/2026-04-28-instagram-scrapling-scraper-hardening.md`.
- `ScraplingRuntime.healthcheck()` still returns healthy when Scrapling imports, while endpoint methods raise `NotImplementedError`.
- The dispatcher skips unhealthy runtimes and only falls through endpoint calls on `RuntimeUnsupported`.
- `InstagramAuthSession` has required fields beyond the old invalid test snippet.
- `pg.fetch_one` accepts a `conn` keyword argument, so comments cancellation can reuse the existing persistence connection.

## Revisions Applied

- Replaced old Superpowers execution language with `orchestrate-subagents` execution governance.
- Rewrote the plan into the `write-plan` schema sections: `summary`, `project_context`, `assumptions`, `goals`, `non_goals`, `phased_implementation`, `architecture_impact`, `data_or_api_impact`, `ux_admin_ops_considerations`, `validation_plan`, `acceptance_criteria`, `risks_edge_cases_open_questions`, `follow_up_improvements`, `recommended_next_step_after_approval`, and `ready_for_execution`.
- Added an exact `ADDITIONAL SUGGESTIONS` phase.
- Integrated all ten prior suggestions as required tasks with dependencies, affected surfaces, validation, acceptance criteria, and commit boundaries.
- Added branch and no-worktree stop rules from `orchestrate-subagents`.
- Added final completion report fields required by `orchestrate-subagents`.

## Approval Decision

Use `.plan-grader/instagram-scrapling-scraper-hardening-revised-schema-20260428-165102/REVISED_PLAN.md` as the execution source after branch preflight is resolved.

Implementation should not start through `orchestrate-subagents` while the checkout is on `chore/backend-batch-2026-04-28` unless the user explicitly approves continuing there.

## Remaining Risks

- The plan is execution-ready but branch-conditional because `orchestrate-subagents` expects `main`.
- The additional suggestions increase scope; subagent ownership boundaries must be enforced to avoid overlapping edits.
- Live Instagram smoke remains optional and can be blocked by auth, challenge, proxy, or dependency drift.
