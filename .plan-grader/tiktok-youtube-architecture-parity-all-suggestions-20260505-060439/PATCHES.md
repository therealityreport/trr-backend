# Patches

This revision incorporates every numbered suggestion from `.plan-grader/tiktok-youtube-architecture-parity-20260505-055823/SUGGESTIONS.md` into the execution plan.

## Required Plan Patches

1. Added the exact `## ADDITIONAL SUGGESTIONS` phase.
2. Promoted all ten prior suggestions from optional ideas to accepted implementation tasks.
3. Added task-level dependencies, affected files, validation, acceptance criteria, and commit boundaries for each suggestion.
4. Preserved the no-schema, no-route, no-comments-lane, and no-new-YouTube-worker-lane boundaries.
5. Kept `recommendedNextExecutionSkill` as `orchestrate-subagents`.

## Suggestion Mapping

| Source suggestion | Revised plan task |
| --- | --- |
| 1. Add Social Run Metadata Fixtures | `ADDITIONAL SUGGESTIONS`, Task 1 |
| 2. Add A One-Page Operator Runbook | `ADDITIONAL SUGGESTIONS`, Task 2 |
| 3. Add Error-Code Registry Tests | `ADDITIONAL SUGGESTIONS`, Task 3 |
| 4. Add YouTube Catalog Golden Fixture | `ADDITIONAL SUGGESTIONS`, Task 4 |
| 5. Add TikTok Fetcher Runtime Metadata Snapshot | `ADDITIONAL SUGGESTIONS`, Task 5 |
| 6. Add A Local Smoke Wrapper | `ADDITIONAL SUGGESTIONS`, Task 6 |
| 7. Document Batch-Upsert Decision | `ADDITIONAL SUGGESTIONS`, Task 7 |
| 8. Add Import-Cycle Check | `ADDITIONAL SUGGESTIONS`, Task 8 |
| 9. Add Queue Dashboard Copy Check | `ADDITIONAL SUGGESTIONS`, Task 9 |
| 10. Add 30-Day Follow-Up Query | `ADDITIONAL SUGGESTIONS`, Task 10 |

## Requirement Trace

| Requirement | Revised plan location |
| --- | --- |
| Improve TikTok architecture | Phase 1, Phase 2 |
| Improve YouTube architecture | Phase 3 |
| Apply Instagram hardening patterns | Phase 1, Phase 4 |
| Include all suggestions | `ADDITIONAL SUGGESTIONS` |
| Avoid new comments lane | Non-Goals, Stop Rules |
| Avoid schema/route/worker-stage changes | Non-Goals, Stop Rules |
| Use subagents for implementation | Recommended Handoff |
