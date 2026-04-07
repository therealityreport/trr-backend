# Instagram Backfill Worker Reliability — Task 27 Plan

Repo: TRR-Backend
Last updated: 2026-04-07

## Goal
Restore reliable Modal-owned Instagram shared-account backfills and add proactive local auth repair before Instagram cookies expire or checkpoint.

## Status Snapshot
The backend implementation is in progress on `main`. The local `.env` already enables the Modal job plane, the backend already has queue/runtime enforcement for Modal-owned shared-account work, and this task focuses on making route execution mode explicit, surfacing an Instagram auth repair signal, persisting cookie refresh metadata, and automating the existing full repair pipeline through a local scheduled worker.

## Scope

### Phase 1: Route Execution Diagnostics
Make shared-account catalog route responses explicit about whether the request entered the queue or used local inline fallback.

Files to change:
- `api/routers/socials.py`
- `tests/api/routers/test_socials_season_analytics.py`

### Phase 2: Instagram Auth Repair Signal
Add a repository helper that combines cookie health and recent scrape-job failures into a single read-only repair signal.

Files to change:
- `trr_backend/repositories/social_season_analytics.py`
- `tests/repositories/test_social_season_analytics.py`

### Phase 3: Cookie Refresh Metadata
Persist the last refresh timestamp in the flat Instagram cookie file format without breaking existing cookie loaders.

Files to change:
- `trr_backend/socials/instagram/cookie_refresh.py`
- `tests/socials/test_cookie_refresh_flows.py`

### Phase 4: Scheduled Local Repair Worker
Create a local-only CLI that polls cookie age and recent auth failures, then invokes the existing full repair pipeline when repair is needed.

Files to change:
- `scripts/socials/cookie_refresh_worker.py`
- `tests/scripts/test_cookie_refresh_worker.py`

### Phase 5: Runbook And Verification
Document the local schedule/operator flow and verify the targeted backend slices.

Files to change:
- `docs/runbooks/social_worker_queue_ops.md`
- `docs/cross-collab/TASK27/STATUS.md`

## Out of Scope
- TRR-APP changes unless a real caller is discovered to be forcing `allow_inline_dev_fallback=true`
- screenalytics changes
- Modal dispatch core logic changes in `trr_backend/modal_dispatch.py` unless code evidence disproves the current diagnosis
- Browser automation changes to run Playwright on Modal

## Locked Contracts
### Social Catalog Admin Routes
Keep `/api/v1/admin/socials/profiles/{platform}/{account_handle}/catalog/*` stable and additive.

### Shared-Account Modal Ownership
Shared-account Instagram catalog jobs remain Modal-owned in normal operation; local inline fallback remains explicit dev-only behavior.

### Repair Pipeline
Reuse the existing `repair_instagram_auth.py` full repair flow rather than creating a second secret-push/deploy/probe implementation.

## Acceptance Criteria
1. Shared-account catalog responses include additive execution diagnostics that distinguish queue execution from inline fallback.
2. Backend code can produce a structured Instagram auth repair signal using cookie validation plus recent queue failures.
3. Instagram cookie refresh metadata is persisted without changing the cookie loader contract used by the scraper.
4. A local-only scheduled repair worker can skip when healthy and run the full repair pipeline when cookies are stale or recent auth failures exist.
5. The backend runbook documents the local schedule, trigger conditions, and inline-fallback guardrails.
6. Targeted backend validation passes and Task 27 status docs capture the implementation and verification state.
