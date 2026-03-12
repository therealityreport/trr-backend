# Plan — Task 11 (Plan A: Workspace + TRR-Backend Contract Completion)

Repo: TRR-Backend
Last updated: March 12, 2026

## Status Snapshot (As of March 12, 2026)

- In progress.
- Public traffic is now on the Render-hosted backend and the rollback observation window is open until March 13, 2026 at 16:09 EDT.
- Better Stack can wait until after that observation window. It is required before CloudWatch is reduced or removed, but it does not block the current Render cutover validation window.
- Logging decision: use Better Stack free first rather than paying up front. Upgrade only if real log volume or retention needs exceed the free tier.
- Next TODO after the observation window closes cleanly:
  - create or use a Better Stack free HTTP source and configure `BETTER_STACK_*` env on the live Render service and Modal job runtime
  - verify backend API and Modal job logs arrive in Better Stack
  - then run `scripts/ops/aws_teardown_pass.py --execute`
  - then reduce CloudWatch usage and proceed with any follow-up cleanup outside the scripted pass

## Scope

Finalize and harden additive backend/workspace contracts so admin SSE jobs are resumable across disconnect/reconnect and workspace startup no longer reloads all matching browser tabs by default.

## Agent A Remote Job Plane Addendum (Render API + Modal job plane)

1. Additive worker-claim lifecycle fields are required on:
   - `core.admin_operations`
   - `core.google_news_sync_jobs`
   - `social.reddit_refresh_runs`
2. Long-job kickoff ownership semantics:
   - `TRR_JOB_PLANE_MODE=local|remote`
   - `TRR_LONG_JOB_ENFORCE_REMOTE=0|1`
3. In remote mode:
   - the public FastAPI host is Render once cutover completes
   - `serve_backend_api` remains the Modal rollback host and parity target during migration
   - TRR-APP targets the public backend host via `TRR_API_URL`
   - Modal functions own async execution for admin, social, reddit, google-news, and vision work
4. Additive kickoff contract fields (frozen for TRR-APP):
   - `execution_owner` (`local_api|remote_worker`)
   - `execution_mode_canonical` (`local|remote`)
5. Legacy worker entrypoints remain rollback/local-dev only:
   - `python -m scripts.workers.admin_operations_worker`
   - `python -m scripts.workers.reddit_refresh_worker`
   - `python -m scripts.workers.google_news_worker`

## March 12, 2026 runtime closeout note

Task 11 is no longer about proving EC2 worker ownership. The final architecture
for this workstream is:

- `TRR-APP` on Vercel Preview/Production
- `TRR_API_URL` pointing at the Render-hosted backend API
- `serve_backend_api` retained as the Modal rollback host and executor-adjacent parity surface
- Modal functions as the live async execution plane
- legacy AWS API/runtime retained only until the rollback window closes

## Frozen Contract Surface

1. `GET /api/v1/admin/operations/{operation_id}`
2. `GET /api/v1/admin/operations/{operation_id}/stream?after_seq={n}`
3. `POST /api/v1/admin/operations/{operation_id}/cancel`
4. Existing admin SSE routes keep existing paths/verbs and now emit additive payload fields:
   - `operation_id`
   - `event_seq`
5. Optional kickoff metadata remains additive:
   - `client_session_id`
   - `client_workflow_id`
6. Optional headers remain additive:
   - `x-trr-tab-session-id`
   - `x-trr-flow-key`
7. Workspace tab-sync contract:
   - `WORKSPACE_BROWSER_TAB_SYNC_MODE=reuse_no_reload|reload_first|reload_all`
   - default `reuse_no_reload`

## Execution Phases (Plan A)

### Phase 0 — Freeze criteria + baseline guard

- Define explicit freeze checks and capture unrelated baseline failures separately.

### Phase 1 — Dedicated operation contract tests

- Added `tests/api/routers/test_admin_operations.py`.
- Added `tests/repositories/test_admin_operations.py`.
- Added explicit operation-envelope replay assertion.

### Phase 2 — Additive SSE producer route assertions

Updated route suites to assert additive operation envelope semantics:
- `tests/api/routers/test_admin_show_sync.py`
- `tests/api/routers/test_admin_show_links.py`
- `tests/api/routers/test_admin_show_bravo.py`
- `tests/api/routers/test_admin_person_images.py`
- `tests/api/routers/test_admin_scrape_contracts.py`
- `tests/api/routers/test_admin_asset_batch_jobs.py`

March 4 completion update:
- Remaining migration surfaces now operation-backed in backend:
  - `admin_show_sync` refresh-photos stream
  - `admin_person_images` refresh-images stream
  - `admin_person_images` reprocess-images stream
- Remote dispatcher now resolves all migrated operation types (including the three above).

### Phase 3 — Runtime guardrails

Added structured logs for:
- create-vs-attach decisions (`operation_type`, `operation_id`, `attached`, `client_session_id`, `client_workflow_id`, `request_id`)
- replay usage (`operation_id`, `after_seq`, `events_replayed`)
- cancel audits (`operation_id`, `prior_status`, `resulting_status`, `request_id`)

### Phase 4 — Workspace mode lock

Verified workspace default mode and messaging remain locked to `reuse_no_reload` unless explicit override.

### Phase 5 — Freeze checkpoint artifacts

- Publish contract examples and Plan B dependency gate in TASK11 docs + backend/app handoff.

### Phase 6 — Plan B integration additive support (completed)

- Add lightweight reddit run listing endpoint (`GET /api/v1/admin/socials/reddit/runs`) for explicit manual-attach UX in TRR-APP run surfaces.
- Keep all existing reddit refresh endpoints backward-compatible and unchanged.

## Plan B Gate

**Do not start Plan B implementation until this checkpoint is consumed from `STATUS.md`:**
- operation contract tests passing,
- additive SSE route assertions present and passing in targeted runs,
- guardrail logs present,
- workspace mode contract verified,
- known unrelated baseline failures documented.

## March 5, 2026 execution checkpoint

- Task 11 staging unblock execution completed operationally (Phase 0-5 runbook actions executed).
- Runtime/network fixes were applied, but required resilience reruns remain blocked on missing kickoff envelope emission in staging.
- Current execution outcome is tracked in `STATUS.md` and evidence root:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/evidence/aws-worker-plane/20260304-195705-task11-unblock`

March 5 continuation:
- Staging DB parity fix was applied on the same evidence run (`0172`/`0173`).
- `30/31/32` reruns progressed from envelope-missing to envelope-present; terminal continuity/replay evidence remains incomplete and gate stays blocked.
