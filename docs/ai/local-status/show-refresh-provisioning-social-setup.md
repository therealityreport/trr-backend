# Show Refresh Provisioning + Social Setup

Last updated: 2026-03-24

## Status
- Backend phase complete.

## What changed
- Expanded `admin_show_sync` refresh targets to include `videos`, `news`, and `social_setup`.
- Added inline refresh helpers for:
  - Bravo video sync
  - Google News sync
  - Bravo social-target seeding
- Streamed refresh progress now recognizes the expanded target set.
- Added targeted backend tests for the expanded refresh contract.

## Validation
- Passed: `pytest tests/api/routers/test_admin_show_sync.py -q`
- Passed: `ruff check api/routers/admin_show_sync.py tests/api/routers/test_admin_show_sync.py`

## Notes
- `social_setup` seeds Bravo official-analysis targets via the existing season social analytics defaults, which include the shared Bravo/WWHL account set.
- Reddit auto-seeding remains app-orchestrated from the show admin refresh flow.

## Handoff Snapshot
```yaml
handoff:
  include: false
  state: archived
  last_updated: 2026-03-24
  current_phase: "archived continuity note"
  next_action: "See newer continuity notes if follow-up is needed"
  detail: self
```
