# Workspace disk reclamation guardrails

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

- Added `scripts/cleanup-workspace-disk.py` and the `make cleanup-disk` target.
- The YouTube scrape CLI now defaults downloads to an external cache path unless explicitly overridden.
