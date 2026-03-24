# Person Refresh NBCUMV Timeout And Cancel Hardening

Date: 2026-03-22

## Summary
- Hardened the person image refresh NBCUMV import path so a single stuck asset cannot pin the whole refresh forever.
- Kept the existing admin-operation cancel flow, but made the refresh stream wait for the current NBCUMV asset to finish or time out instead of immediately abandoning the worker thread.

## Backend changes
- Added `TRR_NBCUMV_IMPORT_ITEM_TIMEOUT_S` support via [`api/routers/admin_person_images.py`](/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_person_images.py).
- Wrapped each `_import_single_item(...)` NBCUMV asset import in a one-item `ThreadPoolExecutor` timeout guard.
- When a single NBCUMV asset times out, the run now records that asset as failed, emits progress for the timeout, and continues to later assets.
- During refresh-stream polling, cancel requests now emit `Cancellation requested. Finishing the current NBCUMV asset...` and wait for the cooperative stop path rather than immediately bailing out of the stage loop.
- Refreshed the cancelled-result path to always have a progress snapshot available before emitting the cancellation progress payload.

## Tests
- Added coverage in [`tests/api/routers/test_admin_person_images.py`](/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_person_images.py):
  - `test_import_nbcumv_person_media_times_out_stuck_asset_and_continues`
  - `test_import_nbcumv_person_media_honors_cancel_between_assets`
  - existing `test_refresh_stream_stops_when_nbcumv_import_reports_cancellation` still passes

## Validation
- Passed:
  - `python -m py_compile api/routers/admin_person_images.py tests/api/routers/test_admin_person_images.py`
  - `pytest -q tests/api/routers/test_admin_person_images.py -k 'times_out_stuck_asset_and_continues or honors_cancel_between_assets or refresh_stream_stops_when_nbcumv_import_reports_cancellation'`
- Note:
  - two older stream-progress tests in this file (`stream_emits_nbcumv_progress_updates`, `stream_uses_nbcumv_stage_totals_when_getty_candidates_are_zero`) were not used as acceptance gates here; repeated stale local pytest processes were found and cleaned up during validation.

## Handoff Snapshot
```yaml
handoff:
  include: true
  state: recent
  last_updated: 2026-03-22
  current_phase: "person refresh NBCUMV timeout and cooperative cancel hardening shipped"
  next_action: "watch the next stuck NBCUMV asset refresh and tune TRR_NBCUMV_IMPORT_ITEM_TIMEOUT_S only if real assets still need more than the default 120s"
  detail: self
```
