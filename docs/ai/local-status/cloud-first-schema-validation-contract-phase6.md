# Cloud-First Schema Validation Contract Phase 6

Date: 2026-04-03

## Handoff Snapshot
```yaml
handoff:
  include: true
  state: recent
  last_updated: 2026-04-03
  current_phase: "phase 6 backend validation contract updated"
  next_action: "Use the frozen remote-first validation contract when Phase 7 aligns backend defaults and workspace scripts."
  detail: self
```

## Backend Contract Shift
- `TRR-Backend/Makefile` now documents isolated remote branch/disposable database validation as the preferred schema-doc verification path.
- `TRR-Backend/docs/README_local.md` now gives a concrete remote-first sequence: set `TRR_DB_URL` to an isolated target, push migrations there, then run `make schema-docs-check`.
- Local Docker-backed replay remains available through `make schema-docs-reset-check`, but it is now documented as explicit fallback rather than the default milestone verification path.

## Safety Rules
- Use isolated remote branch/disposable database targets for destructive migration validation.
- Do not aim replay or reset verification at production or long-lived shared persistent databases.
- Keep `TRR_DB_URL` pointed at the exact isolated target you intend to validate.
