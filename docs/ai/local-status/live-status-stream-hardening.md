# Live status stream hardening

Last updated: 2026-04-11

## Handoff Snapshot
```yaml
handoff:
  include: false
  state: recent
  last_updated: 2026-04-11
  current_phase: "live-status stream moved off the event loop and request-timeout stream exemptions narrowed"
  next_action: "start seam extraction for queue-status and admin-operations reads out of the social ingest monolith"
  detail: self
```

## What changed

- `/api/v1/admin/socials/live-status/stream` now builds payloads via the existing threadpool helper instead of calling synchronous repository code directly inside the async SSE loop.
- `trr_backend/middleware/request_timeout.py` no longer exempts every path ending in `/stream`.
- Timeout exemptions for streaming are now limited to the known long-lived SSE endpoints identified in the backend route inventory.

## Compatibility sweep

- `TRR-APP` was already using signed internal-admin JWTs for backend proxy calls; no app code changes were required.
- `screenalytics` outbound TRR calls already prefer signed internal-admin JWTs as well; targeted unit tests confirmed the current path remains compatible.

## Validation snapshot

- `python -m pytest tests/middleware/test_request_timeout.py -q -k "known_stream_endpoint_exempt or unknown_stream_endpoint_not_exempt"` -> `2 passed`
- `python -m pytest tests/api/routers/test_socials_season_analytics.py -q -k "live_status_stream_uses_threadpool or get_live_status_aggregates_health_queue_and_operations"` -> `2 passed`
- `pytest -q /Users/thomashulihan/Projects/TRR/screenalytics/tests/unit/test_internal_admin_auth.py /Users/thomashulihan/Projects/TRR/screenalytics/tests/unit/test_trr_ingest.py -k "internal_admin or service_headers"` -> `9 passed`
- `ruff check` on the touched backend files -> clean
- `ruff format --check` on the touched backend files -> clean

## Notes

- This slice intentionally did not change the non-stream `/live-status` endpoint because the event-loop risk was isolated to the SSE path.
- The next backend concern remains architectural rather than operational: reducing the `social_season_analytics.py` hotspot by extracting queue-status and admin-operations seams.
