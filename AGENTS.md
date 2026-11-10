# TRR-BACKEND INSTRUCTIONS

Inherit `../AGENTS.md`; it is authoritative for shared workspace policy, including the managed Project knowledge link.

## Scope
- Backend-only instructions for this directory.

## Cross-Boundary Triggers
- When API, schema, auth, or shared worker behavior changes, update the current contract under `../docs/` and complete the app follow-through in the same session.
- Keep backend-first ordering for shared contracts.

## Non-Negotiable Rules
- Keep schema, API, auth, worker, scraper, and job contracts aligned.
- Validate live-runtime assumptions against code, tests, or current runtime state.
- Follow the root Modal completion rule for Modal-affecting changes.

## Validation
- Run the smallest relevant backend check:
  - Targeted tests: `.venv/bin/python -m pytest tests/<module>.py`. Add `-m "not browser and not vision and not live"` for the offline lane; `pytest.ini` defines these markers.
  - Schema changes: `make schema-docs-check`.
  - Full local replay (Docker and Supabase): `make ci-local`. Use it only when the task needs it.
- When SQL ownership changes, validate the SQL path and report its status as required by `../AGENTS.md`.
