# Other Projects — Task 24 (Final Supabase connection audit and donor transition inventory)

Repo: TRR-Backend
Last updated: 2026-04-02

## Cross-Repo Snapshot
- TRR-Backend: Runtime DB contract already canonical; this task captures the cross-repo matrix and the backend-side migration inputs. See TRR-Backend TASK24.
- screenalytics: Transition runtime wording cleaned up; donor/runtime/retire split documented. See screenalytics TASK13.
- TRR-APP: Runtime DB contract already canonical; docs now classify Supabase envs accurately and remove stale app `SCREENALYTICS_API_URL`. See TRR-APP TASK23.

## Responsibility Alignment
- TRR-Backend
  - Own the canonical runtime DB contract and the dependency inventory the DeepFace reset must remove.
  - Own the backend-side list of active `screenalytics` HTTP and `screenalytics.*` storage dependencies.
- screenalytics
  - Stay truthful as a transitional runtime.
  - Expose the donor modules and docs that must survive the reset.
- TRR-APP
  - Preserve the app-owned admin entry points and correctly classify which Supabase and screenalytics dependencies are actually live.

## Dependency Order
1. TRR-Backend audit and contract confirmation
2. screenalytics transition-runtime and donor inventory cleanup
3. TRR-APP env/docs cleanup and app-facing dependency checklist

## Locked Contracts (Mirrored)
- Runtime Postgres precedence is `TRR_DB_URL` then `TRR_DB_FALLBACK_URL`.
- Default runtime lane is Supavisor session mode on `pooler.supabase.com:5432`.
- `TRR-APP` server-admin Supabase access is active today.
- `TRR-APP` browser Supabase is Flashback-scoped, not app-global.
- `screenalytics` is treated as both a transitional runtime and a donor repo until the DeepFace reset lands.
