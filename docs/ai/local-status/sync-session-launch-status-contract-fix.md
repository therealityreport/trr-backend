# Sync-session launch status contract fix

Last updated: 2026-03-24

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

- `create_sync_session` now preserves request-level status `created` and `attached` instead of overwriting it with internal session state.
- Focused pytest and ruff coverage passed when the fix landed.
