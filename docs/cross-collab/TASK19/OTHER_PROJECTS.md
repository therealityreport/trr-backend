# Other Projects — Task 19 (Env contract migration and local startup recovery)

Repo: TRR-Backend
Last updated: 2026-03-30

## Cross-Repo Snapshot
- TRR-Backend: local startup repaired, runtime DB precedence normalized, validator/report path wired.
- TRR-APP: launcher-owned local URL precedence active, app env examples normalized, Vercel env review completed.
- screenalytics: local runtime classification aligned with backend, env examples and CI checks updated.

## Responsibility Alignment
- TRR-Backend
  - Own startup classification, deployed-secret gates, runtime DB env precedence, and Render/Modal contract enforcement.
- TRR-APP
  - Own app-side backend base contract (`TRR_API_URL`) and Vercel runtime env cleanup.
- screenalytics
  - Mirror backend local/dev sentinel behavior and use canonical runtime DB names.

## Dependency Order
1. TRR-Backend
2. screenalytics
3. TRR-APP

## Locked Contracts (Mirrored)
- `TRR_LOCAL_DEV=1` is the single authoritative local-workspace sentinel.
- `TRR_API_URL` is launcher-owned during `make dev`.
- `TRR_DB_URL` with optional `TRR_DB_FALLBACK_URL` is the canonical runtime DB contract.
- Survey cutover is no longer blocked by unknown Vercel envs; retained live vars are documented in `docs/workspace/vercel-env-review.md`.
