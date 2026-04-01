# Other Projects — Task 17 (Supabase trust-boundary hardening)

Repo: TRR-Backend
Last updated: 2026-03-30

## Cross-Repo Snapshot
- TRR-Backend: complete for the backend-owned auth/read-surface slice.
- TRR-APP: complete for show-list migration and Flashback admin server routing; survey schema unification remains deferred.
- screenalytics: complete for strict non-local DB/token enforcement and canonical DB resolver usage.

## Responsibility Alignment
- TRR-Backend
  - Own the internal admin JWT contract, startup auth-env enforcement, and canonical shared read surfaces.
- TRR-APP
  - Consume backend-owned read surfaces and move Flashback admin writes behind server routes.
- screenalytics
  - Fail closed in non-local environments when DB/service token are missing and stop silently omitting backend auth.

## Dependency Order
1. TRR-Backend
2. screenalytics
3. TRR-APP

## Locked Contracts (Mirrored)
- Keep shared contracts aligned with owning repo PLAN.md.
- Survey schema ownership is still unresolved for execution. Do not blindly switch app survey reads/writes to backend `surveys.*` until a compatibility migration exists.
