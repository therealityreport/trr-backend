# Workspace disk reclamation guardrails

Last updated: 2026-03-16

## Handoff Snapshot
```yaml
handoff:
  include: true
  state: recent
  last_updated: 2026-03-16
  current_phase: "backend/workspace phase complete"
  next_action: "Only rerun the workspace cleanup command when .next or local social download dumps regrow materially"
  detail: self
```

- Added `scripts/cleanup-workspace-disk.py` and the `make cleanup-disk` target.
- The YouTube scrape CLI now defaults downloads to an external cache path unless explicitly overridden.
