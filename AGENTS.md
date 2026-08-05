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

<!-- project-manager:graphify:start -->
## graphify

- Check task-relevant graph freshness before using Graphify evidence.
- When an existing graph is stale because relevant code changed, automatically refresh it locally only after the safety preview passes.
- Never create a missing graph automatically, use a network or LLM backend, or use stale graph evidence.
- If refresh is blocked, fails, or a semantic-document layer is stale, continue from current project files and report that Graphify evidence was omitted or partial.
- Keep lifecycle hooks read-only and non-mutating; they report freshness but never rebuild graphs.
- Keep app-managed transient planning and backup directories outside the corpus via `.graphifyignore`.
- Keep `graphify-out/` local and ignored by Git.
<!-- project-manager:graphify:end -->
