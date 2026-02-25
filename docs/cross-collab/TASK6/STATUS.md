# Status — Task 6 (Admin “Sync/Refresh Shows” Endpoints)

Repo: TRR-Backend
Last updated: February 24, 2026

## Phase Status

| Phase | Description | Status | Notes |
|------:|-------------|--------|-------|
| 1 | Admin list sync endpoint | Implemented | `POST /api/v1/admin/shows/sync-from-lists` in `api/routers/admin_show_sync.py`. |
| 2 | Per-show refresh endpoint | Implemented | `POST /api/v1/admin/shows/{show_id}/refresh` runs sync script mains synchronously. |
| 2.5 | Per-show refresh stream (SSE) | Implemented | `POST /api/v1/admin/shows/{show_id}/refresh/stream` streams progress + complete. |
| 3 | Wire router into app | Implemented | `api/main.py` includes router under `/api/v1`. |
| 4 | Tests | Implemented | `tests/api/routers/test_admin_show_sync.py`. |

## Blockers

None.

## Recent Activity

- February 24, 2026: RHOSLC person-stream closeout stabilization completed.
  - Updated `api/routers/admin_person_images.py`:
    - refresh/reprocess streams now emit immediate `starting` progress with additive `request_id` echo.
    - refresh stream imports are lazy-loaded after first event.
    - long blocking profile/source and reprocess stage calls now emit heartbeat progress (`heartbeat`, `elapsed_ms`) via threaded loops.
  - Added/updated stream tests in `tests/api/routers/test_admin_person_images.py`:
    - startup event assertions,
    - request-id echo assertions,
    - heartbeat assertions for slow source/reprocess stages.
  - Validation:
    - `python -m py_compile /Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_person_images.py` (pass)
    - `pytest /Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_person_images.py` (pass, `20 passed`)
  - Runtime evidence:
    - normal: `/Users/thomashulihan/Projects/TRR/.logs/manual-e2e/rhoslc-normal/person-refresh-stream-sample.sse`
    - normal: `/Users/thomashulihan/Projects/TRR/.logs/manual-e2e/rhoslc-normal/person-reprocess-stream-sample.sse`
    - degraded: `/Users/thomashulihan/Projects/TRR/.logs/manual-e2e/rhoslc-degraded/person-refresh-stream-sample.sse`
  - RHOSLC closeout status: blocker cleared, Go/No-Go = **GO**.

- February 10, 2026: Added admin show sync router + endpoints + tests; wired into `api/main.py`.
- February 24, 2026: RHOSLC stream closeout stabilization follow-up:
  - `refresh/stream` now emits initial SSE progress before DB/step expansion, with heartbeat preserved for long-running steps.
  - `refresh-photos/stream` now emits initial SSE progress before show lookup/setup work.
  - Updated stream test expectations for cast precondition failures to SSE error-event semantics.
  - Validation:
    - `python -m py_compile api/routers/admin_show_sync.py` (pass)
    - `pytest tests/api/routers/test_admin_show_sync.py -k 'refresh_stream or refresh_photos_stream'` (`6 passed`)
