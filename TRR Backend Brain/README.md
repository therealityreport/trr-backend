# TRR-Backend Brain

Scope:
- FastAPI routes
- backend domain logic
- schema and migration ownership
- operational backend tooling

Carry forward from the previous repo `AGENTS.md`:
- backend changes land before downstream app follow-through
- API prefix remains `/api/v1`
- migrations are additive and never rewritten
- validate with `ruff check .`, `ruff format --check .`, `pytest -q`, and `make schema-docs-check` when schema changes
