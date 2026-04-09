# Concerns remediation and Screenalytics contract lock — Task 28 Plan

Repo: TRR-Backend
Last updated: 2026-04-09

## Goal
Lock the retirement path for `screenalytics`, migrate backend-owned contracts forward, and remove the long-term backend assumption that Screenalytics remains a permanent peer runtime.

## Status Snapshot
In progress. Retirement end-state locked: `screenalytics` is a shrinking transitional runtime whose surviving behavior must migrate into `TRR-Backend` or `TRR-APP`.

## Scope

### Phase 1: Contract lock + backend-first cutover
Document the retirement decision, switch Screenalytics-facing backend routes to internal-admin-first auth, and introduce uncapped gallery pagination contracts that downstream app routes can adopt.

Files to change:
- `api/screenalytics_auth.py` — remove dual-auth-by-default behavior; keep service-token fallback as explicit transition-only opt-in.
- `api/routers/admin_show_reads.py` — expose cursor pagination for show and season asset reads.
- `trr_backend/repositories/admin_show_reads.py` — continue repo-owned asset reads while app-facing contracts move to cursor semantics.
- `tests/api/test_screenalytics_ingest_endpoints.py`
- `tests/api/test_screenalytics_runs_v2.py`

### Phase 2: Backend-owned migration follow-through
Keep backend-owned vision, cast screentime, and media admin flows as the canonical destination while transitional Screenalytics consumers are updated in the same session.

Files to change:
- `api/routers/admin_cast_screentime.py` only if required to keep internal Screenalytics workers functional during retirement.

## Out of Scope
- Deleting every Screenalytics compatibility route in one pass without first updating its remaining callers.
- Rewriting unrelated backend domains that are not part of the retirement or gallery contract work.

## Locked Contracts
- `TRR_INTERNAL_ADMIN_SHARED_SECRET` is the canonical internal auth for surviving Screenalytics-facing backend routes.
- `SCREENALYTICS_SERVICE_TOKEN` remains transitional only and must never be the default auth assumption.
- Show and season gallery reads move to a cursor contract with downstream app adoption in the same task.

## Acceptance Criteria
1. Backend Screenalytics-facing routes no longer rely on service-token fallback by default.
2. Show and season asset endpoints expose cursor pagination with no fixed 15k retrieval ceiling.
3. Downstream Screenalytics and TRR-APP consumers are updated in the same task when backend contracts change.
4. `ruff check . && ruff format --check . && pytest -q` pass for `TRR-Backend`.
5. Task docs remain synchronized across repos.
