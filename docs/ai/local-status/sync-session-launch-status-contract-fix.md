# Sync-session launch status contract fix

Last updated: 2026-03-16

## Handoff Snapshot
```yaml
handoff:
  include: true
  state: recent
  last_updated: 2026-03-16
  current_phase: "backend fix complete"
  next_action: "Monitor only unless another sync-session launch status regression appears in app smoke"
  detail: self
```

- `create_sync_session` now preserves request-level status `created` and `attached` instead of overwriting it with internal session state.
- Focused pytest and ruff coverage passed when the fix landed.
