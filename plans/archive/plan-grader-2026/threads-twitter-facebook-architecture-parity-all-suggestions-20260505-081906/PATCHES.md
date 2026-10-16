# Revision Patches: All Suggestions Incorporated

Source package: `.plan-grader/threads-twitter-facebook-architecture-parity-20260505-081358/`
New plan: `.plan-grader/threads-twitter-facebook-architecture-parity-all-suggestions-20260505-081906/REVISED_PLAN.md`

## Summary

All twelve prior numbered suggestions are now accepted requirements. They are not left as optional notes. Each is mapped to a concrete task under `## ADDITIONAL SUGGESTIONS` in `REVISED_PLAN.md`.

## Suggestion Mapping

| Source suggestion | Revised plan task | Concrete plan change |
| --- | --- | --- |
| 1. Remote auth smoke wrapper | Task 1 | Adds required smoke wrapper/docs after Phase 1 readiness support. |
| 2. Fixture secret validator | Task 2 | Adds pytest-backed fixture validator for run metadata fixtures. |
| 3. Wrapper ledger entries | Task 3 | Requires ledger entries with owner Module and deletion criteria. |
| 4. Operator query snippet | Task 4 | Requires copyable remote-auth failure query snippet. |
| 5. Expanded import-cycle checks | Task 5 | Extends import-cycle coverage to all current `posts_catalog` Modules. |
| 6. Fake persistence adapter | Task 6 | Requires shared fake persistence helper for catalog tests where practical. |
| 7. Golden metadata fixtures | Task 7 | Requires one metadata golden fixture per platform. |
| 8. Conditional benchmark | Task 8 | Makes benchmark conditional on material runtime path change. |
| 9. Batch-upsert checklist | Task 9 | Adds future equivalence checklist while keeping batch upsert deferred. |
| 10. Wrapper cleanup plan | Task 10 | Adds future cleanup plan for retiring compatibility wrappers. |
| 11. Modal smoke command section | Task 11 | Adds copyable Modal readiness commands to runbook. |
| 12. No-new-lane review question | Task 12 | Adds architecture review checklist question for Twitter/X and Facebook lanes. |

## Other Plan Changes

- Replaced the prior optional-suggestions framing with accepted implementation requirements.
- Added exact archive and cleanup sections required by the Plan Grader artifact contract.
- Preserved the orchestration handoff to `orchestrate-subagents`.
- Kept no-schema, no-route, no-stage, no-worker-lane, and no-secret-leak stop rules.
