# Comparison: Prior Revised Plan vs All-Suggestions Plan

## Inputs

- Prior revised plan: `.plan-grader/threads-twitter-facebook-architecture-parity-20260505-081358/REVISED_PLAN.md`
- Prior suggestions: `.plan-grader/threads-twitter-facebook-architecture-parity-20260505-081358/SUGGESTIONS.md`
- New revised plan: `.plan-grader/threads-twitter-facebook-architecture-parity-all-suggestions-20260505-081906/REVISED_PLAN.md`

## Score Movement

| Plan | Score | Rating | Notes |
| --- | ---: | --- | --- |
| Prior revised plan | 92.14 | Ready to execute | Strong execution plan; suggestions were optional. |
| All-suggestions plan | 93.40 | Ready to execute | Slightly higher durability and operator-readiness coverage; broader scope increases validation cost. |

## What Changed

- Every numbered suggestion is now a required task under `ADDITIONAL SUGGESTIONS`.
- Docs and fixtures now carry more explicit acceptance criteria.
- Import-cycle coverage is expanded to every current `posts_catalog` Module.
- A fixture secret validator is now required.
- A compatibility wrapper ledger and future cleanup plan are now required.
- Runtime benchmark work is explicitly conditional, not mandatory.

## Risk Delta

- Durability risk decreased because fixtures, docs, wrapper ledger entries, and review checklist items are now required.
- Scope risk increased slightly because the implementation now includes twelve additional tasks.
- The plan controls that risk by making benchmark work conditional and keeping batch upsert deferred.

## Execution Recommendation

Use the all-suggestions `REVISED_PLAN.md` as the approved implementation source.
