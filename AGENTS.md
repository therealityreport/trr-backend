# TRR-BACKEND INSTRUCTIONS

Inherit `../AGENTS.md`; it is authoritative for shared workspace policy.

## Scope
- Backend-only instructions for /Users/thomashulihan/Projects/TRR/TRR-Backend.

## Cross-Boundary Triggers
- When API, schema, auth, or shared worker behavior changes, update the current contract under `../docs/` and complete the app follow-through in the same session.
- Keep backend-first ordering for shared contracts.

## Non-Negotiable Rules
- Keep schema, API, auth, worker, scraper, and job contracts aligned.
- Validate live-runtime assumptions against code, tests, or current runtime state.
- Follow the root Modal completion rule for Modal-affecting changes.

## Validation
- Run the smallest relevant backend checks from the existing `Makefile`, `pytest.ini`, or targeted test modules.
- When SQL ownership changes, validate the SQL path and report its status as required by `../AGENTS.md`.
