# Env contract migration and local startup recovery — Task 19 Plan

Repo: TRR-Backend
Last updated: 2026-03-30

## Goal
Restore local workspace startup, re-lock runtime env contracts, and migrate backend/runtime tooling onto canonical DB and URL names before any live env cleanup.

## Status Snapshot
Implementation complete. Backend-local validation is green. Live env cleanup proceeded only through inventory/classification where deletion was not provably safe.

## Scope
### Phase 1: Restore local workspace startup
- Accept `TRR_LOCAL_DEV=1` as the authoritative local-workspace sentinel.
- Keep deployed-only secret enforcement fail-closed for actual deployed runtimes.

Files to change:
- `api/main.py`
- `tests/test_startup_config.py`

### Phase 2: Re-lock backend runtime DB contracts
- Keep runtime DB precedence on `TRR_DB_URL`, then `TRR_DB_FALLBACK_URL`.
- Allow `DATABASE_URL` and `SUPABASE_DB_URL` only as explicit tooling or transitional compatibility inputs where still required.

Files to change:
- `trr_backend/db/connection.py`
- `scripts/_db_url.py`
- `scripts/db/run_sql.sh`
- `scripts/verify/verify_schema.py`
- `scripts/supabase/generate_schema_docs.py`
- sync, media, socials, and ops scripts that previously assumed legacy DB names

### Phase 3: Validation and live-runtime inventory
- Add regression coverage for local-vs-deployed startup requirements.
- Inventory Render and Modal runtime surfaces before mutation.
- Stop at classification if live values are unknown or integration-managed.

## Out of Scope
- Survey consumer cutover work beyond baseline env-contract recovery.
- Blind deletion of live env keys that cannot be proven safe.

## Locked Contracts
- `TRR_LOCAL_DEV=1` means a workspace-launched local process and disables deployed-only startup secret requirements.
- Runtime DB contract is `TRR_DB_URL` with optional `TRR_DB_FALLBACK_URL`.
- `SUPABASE_DB_URL` is deprecated and never the preferred runtime input.
- `DATABASE_URL` remains tooling-only.

## Acceptance Criteria
1. Local workspace backend startup succeeds without deployed-only secrets when launched via the workspace sentinel.
2. Deployed-like startup still fails when required deployed-only secrets are absent.
3. Backend scripts and runtime code prefer canonical DB env names.
4. Backend validation passes and live runtime cleanup does not outrun inventory/rollback gates.
