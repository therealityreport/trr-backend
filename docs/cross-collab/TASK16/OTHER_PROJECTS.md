# Other Projects — Task 16 (Instagram catalog gap analysis and operator guidance)

Repo: TRR-Backend
Last updated: 2026-03-30

## Cross-Repo Snapshot
- TRR-Backend: Added gap-analysis classifier and admin route.
- TRR-APP: Added proxy route, admin banner guidance, and CTA wiring for recommended recovery actions.
- screenalytics: No changes required.

## Responsibility Alignment
- TRR-Backend
  - Own owner-vs-catalog classification logic and the additive `/catalog/gap-analysis` API.
- TRR-APP
  - Own operator guidance banner, CTA priority, and bounded-window repair invocation.
- screenalytics
  - No ownership change for this task.

## Dependency Order
1. TRR-Backend
2. screenalytics
3. TRR-APP

## Locked Contracts (Mirrored)
- Keep shared contracts aligned with owning repo PLAN.md.
