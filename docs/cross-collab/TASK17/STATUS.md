# Status — Task 17 (Supabase trust-boundary hardening)

Repo: TRR-Backend
Last updated: 2026-03-30

## Phase Status

| Phase | Description | Status | Notes |
|---|---|---|---|
| 1 | Implementation | Complete | Internal admin JWT contract, startup auth-env enforcement, show list surface, and stale helper cleanup landed. |

## Blockers
- Survey schema unification is intentionally deferred. `TRR-APP` still expects a different survey table shape than the baseline backend `surveys.*` schema.

## Recent Activity
- 2026-03-30: Replaced internal admin verification with signed JWT validation and allowed `internal_admin` in admin-gated flows.
- 2026-03-30: Hardened startup config to fail closed in non-local runtimes when required auth envs are missing.
- 2026-03-30: Added canonical `/api/v1/shows/list` backend surface so the app can delete its direct `core` Supabase lane.
- 2026-03-30: Updated targeted tests and docs for the new internal auth contract.
