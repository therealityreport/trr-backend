# Other Projects — Task 18 (Social backfill remediation for Instagram and TikTok)

Repo: TRR-Backend
Last updated: 2026-03-30

## Cross-Repo Snapshot
- TRR-Backend: Implemented and targeted-validated in this session. See `TRR-Backend/docs/cross-collab/TASK18/`.
- TRR-APP: No follow-up required in this pass. No shared public contract consumed by TRR-APP changed.
- screenalytics: No follow-up required in this pass. No runtime DB or HTTP consumer contract used by screenalytics changed.

## Responsibility Alignment
- TRR-Backend
  - Owns the backfill repository logic, backfill scripts, queue/orchestration behavior, migration for Instagram retry-state columns, and backend-only validation/tests.
- TRR-APP
  - No changes in this task.
- screenalytics
  - No changes in this task.

## Dependency Order
1. TRR-Backend implements and validates the remediation.
2. screenalytics remains unchanged because no producer contract it depends on changed.
3. TRR-APP remains unchanged because no producer contract it depends on changed.

## Locked Contracts (Mirrored)
- Keep Instagram metadata success/failure timestamps semantically distinct.
- Keep TikTok canonical URL and saves fallback behavior centralized in backend helpers.
- Keep shared catalog resume and inline-worker semantics backend-local and additive.
