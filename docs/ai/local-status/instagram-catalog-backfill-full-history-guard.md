# Instagram Catalog Backfill Full History Guard

Last updated: 2026-03-30

## Status
- Backend phase complete.

## What changed
- Hardened the social account catalog backfill router so `full_history` requests explicitly clear `date_start` and `date_end` before calling the repository layer.
- Kept bounded-window behavior unchanged so windowed backfills still forward their supplied dates.
- Added route tests proving stray dates are ignored for `full_history` and forwarded for `bounded_window`.

## Validation
- Passed: `pytest -q /Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_socials_season_analytics.py -k 'catalog_backfill or catalog_sync_recent'`
- Passed: `pytest -q /Users/thomashulihan/Projects/TRR/TRR-Backend/tests/repositories/test_social_season_analytics.py -k 'get_active_social_account_catalog_run or get_social_account_catalog_run_progress or start_social_account_catalog_backfill'`

## Handoff Snapshot
```yaml
handoff:
  include: false
  state: archived
  last_updated: 2026-03-30
  current_phase: "archived continuity note"
  next_action: "Refer to newer status notes if follow-up work resumes on this thread."
  detail: self
```
