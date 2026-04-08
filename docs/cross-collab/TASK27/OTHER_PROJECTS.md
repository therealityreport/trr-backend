# Other Projects — Task 27 (Instagram Backfill Worker Reliability)

Repo: TRR-Backend
Last updated: 2026-04-07

## Cross-Repo Snapshot
- TRR-Backend: In progress. Backend-only reliability hardening and operator docs are being implemented in Task 27.
- TRR-APP: No code change planned. Existing callers already default `allow_inline_dev_fallback` to `false`; revisit only if backend evidence shows an active caller override.
- screenalytics: Not touched. No shared contract drift is expected from this task.

## Responsibility Alignment
- TRR-Backend
  - Shared-account catalog route diagnostics
  - Instagram auth repair signal helper
  - Cookie refresh metadata tracking
  - Scheduled local repair worker and operator runbook
- TRR-APP
  - No ownership unless a caller contract issue is discovered during backend verification
- screenalytics
  - No ownership in this task

## Dependency Order
1. TRR-Backend implementation and targeted verification
2. Optional downstream follow-up only if backend work reveals caller contract drift

## Locked Contracts (Mirrored)
- Social admin route topology remains stable and additive.
- Shared-account Instagram catalog work stays Modal-owned outside explicit dev-only fallback.
- The repair worker runs locally and reuses the existing full repair pipeline.
