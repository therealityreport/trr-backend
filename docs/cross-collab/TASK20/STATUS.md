# Status — Task 20 (Repair admin internal auth routing)

Repo: TRR-Backend
Last updated: 2026-03-30

## Phase Status

| Phase | Description | Status | Notes |
|---|---|---|---|
| 1 | Backend auth-contract repair | Complete | App-proxied admin router families now use `InternalAdminUser` across the audited show and social surfaces. |
| 2 | Local workspace auth-runtime repair | Complete | `scripts/dev-workspace.sh` now exports a launcher-owned local `TRR_INTERNAL_ADMIN_SHARED_SECRET` and `SCREENALYTICS_SERVICE_TOKEN` into backend startup. |
| 3 | Runtime verification | Complete | Shared social reads and show admin routes now return real data locally; auth failure was removed. |

## Blockers
- No auth/routing blocker remains. Shared ingest reaches the healthy Modal
  worker plane; ongoing concerns are now operational readiness and run
  monitoring, not backend auth-contract mismatch.

## Recent Activity
- 2026-03-30: Task scaffolding created.
- 2026-03-30: Converted app-proxied backend admin route families from `AdminUser` to `InternalAdminUser` where the app server is the intended caller.
- 2026-03-30: Isolated the remaining local failure to workspace startup: backend was running with `TRR_LOCAL_DEV=1` but without `TRR_INTERNAL_ADMIN_SHARED_SECRET`, so shared social routes still raised `Authentication service unavailable`.
- 2026-03-30: Updated `scripts/dev-workspace.sh` so local workspace startup owns and injects a stable launcher-managed internal admin secret and screenalytics service token.
- 2026-03-30: Restarted `make dev` and verified local backend/admin routes now authenticate correctly; shared ingest now fails only on worker availability (`SOCIAL_WORKER_UNAVAILABLE`) instead of auth.
- 2026-03-30: Restored Modal social dispatch readiness, propagated remote social
  auth capabilities into worker-heartbeat metadata, and confirmed shared-account
  Instagram canaries dispatch and run on Modal workers.
