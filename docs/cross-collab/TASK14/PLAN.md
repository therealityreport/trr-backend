# Supabase runtime contract cleanup — Task 14 Plan

Repo: TRR-Backend
Last updated: 2026-03-27

## Goal
Canonicalize runtime Supabase/Postgres configuration so TRR uses `TRR_DB_URL`, keeps `TRR_DB_FALLBACK_URL` as the only intentional fallback, defaults to Supavisor session mode, and exposes connection selection clearly in logs.

## Status Snapshot
Implementation in progress. Backend lands the canonical resolver and pool observability first, then screenalytics, then TRR-APP.

## Scope

### Phase 1: Implement
Implement the backend side of the contract cleanup.

Files to change:
- `trr_backend/db/connection.py`
- `trr_backend/db/pg.py`
- `trr_backend/db/preflight.py`
- `tests/db/test_connection_resolution.py`
- `tests/db/test_pg_pool.py`
- `.env.example`

## Out of Scope
- Items owned by other repos unless explicitly required.

## Locked Contracts
- Runtime resolver precedence: `TRR_DB_URL` -> `TRR_DB_FALLBACK_URL`.
- Default runtime lane: Supavisor session mode on `pooler.supabase.com:5432`.
- Legacy direct-host derivation remains override-only.

## Acceptance Criteria
1. Backend resolver prefers `TRR_DB_URL` and ignores legacy runtime env names.
2. Backend pool logging reports host class, connection class, and application name without leaking secrets.
3. Targeted backend tests pass.
4. Task docs remain synchronized.
