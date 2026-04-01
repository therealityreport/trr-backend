# Supabase trust-boundary hardening — Task 17 Plan

Repo: TRR-Backend
Last updated: 2026-03-30

## Goal
Supabase trust-boundary hardening

## Status Snapshot
Implemented on 2026-03-30 with one explicit follow-up: survey schema unification remains deferred until the app and backend table contracts are aligned.

## Scope

### Phase 1: Implement
Implement the backend-owned trust boundary:
- replace the old shared-secret-plus-service-role expectation with signed internal admin JWT verification
- pin Supabase JWT verification to the configured project/issuer
- fail closed in non-local runtimes when required auth secrets are missing
- provide the canonical lightweight `/api/v1/shows/list` read surface used by TRR-APP
- remove stale runtime guidance that still implied `SUPABASE_*` helper/env usage

Files to change:
- `api/auth.py`
- `api/main.py`
- `api/routers/shows.py`
- `api/deps.py`
- `trr_backend/security/jwt.py`
- `trr_backend/security/internal_admin.py`
- `trr_backend/db/preflight.py`
- `trr_backend/repositories/sync_state.py`
- `tests/api/test_auth.py`
- `tests/api/routers/test_shows.py`
- `tests/test_api_smoke.py`

## Out of Scope
- Items owned by other repos unless explicitly required.
- Survey schema consolidation across `surveys.*`, `firebase_surveys.*`, and app-side normalized survey tables.
- Final Supabase exposed-schema and RLS tightening work that depends on a complete caller inventory after app migration.

## Locked Contracts
- Keep shared API/schema contracts synchronized across affected repos.
- Internal admin callers must present a signed JWT with configured issuer/audience and `scope=internal_admin`.
- App/server consumers should use backend-owned read surfaces instead of direct Supabase `core` reads.

## Acceptance Criteria
1. TRR-Backend changes complete and validated.
2. Cross-repo dependency order is respected.
3. Targeted backend validations pass for the changed auth/read-surface files.
4. Task docs remain synchronized.
