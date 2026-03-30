# Other Projects — Task 14 (Supabase runtime contract cleanup)

Repo: TRR-Backend
Last updated: 2026-03-27

## Cross-Repo Snapshot
- TRR-Backend: Implementing canonical resolver, pool logging, and backend env examples in this task.
- TRR-APP: Follows backend contract after backend lands. See TRR-APP TASK14.
- screenalytics: Follows backend contract after backend lands. See screenalytics TASK9.

## Responsibility Alignment
- TRR-Backend
  - Canonical runtime resolver, connection classification, pool/application-name logging, backend env examples.
- TRR-APP
  - Server resolver precedence, browser env split, app server warnings, app env examples.
- screenalytics
  - Runtime DB helper precedence, startup logging, env examples, and compatibility warnings.

## Dependency Order
1. TRR-Backend
2. screenalytics
3. TRR-APP

## Locked Contracts (Mirrored)
- Runtime precedence is `TRR_DB_URL`, then `TRR_DB_FALLBACK_URL`.
- Default runtime lane is Supavisor session mode.
