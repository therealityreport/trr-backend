# AGENTS — TRR-Backend

Canonical repo policy for `TRR-Backend`. `CLAUDE.md` in this repo stays pointer-only.

Read `../AGENTS.md` first for workspace policy. Use this file for backend-local rules and validation, then return to the root policy whenever work changes shared contracts, secrets, managed Chrome usage, or shared handoff workflow.

## Scope
- `api/` — FastAPI routers and handlers
- `trr_backend/` — shared backend logic, repositories, and clients
- `supabase/` — schema, migrations, and schema docs
- `scripts/` — backend operational helpers

## Non-Negotiable Rules
- Backend owns API, schema, and exposed SQL contract changes first; downstream consumer follow-through happens after backend changes land.
- Prefer additive API changes. If a response shape changes, update affected TRR-APP consumers in the same session.
- Never edit an existing migration; create a new one and keep schema docs in sync.
- If migrations, views, or exposed SQL functions change, keep schema docs current and use `./scripts/reload_postgrest_schema.sh` when PostgREST exposure may be stale.
- Keep screenalytics-facing contracts current in `trr_backend/clients/screenalytics.py`.
- Admin and service-token flows must preserve `ADMIN_EMAIL_ALLOWLIST`, `TRR_INTERNAL_ADMIN_SHARED_SECRET`, and `SCREENALYTICS_SERVICE_TOKEN`.
- For shared changes, backend work lands before screenalytics or app follow-through.

## Validation
- `ruff check .`
- `ruff format --check .`
- `pytest`
- Run `make schema-docs-check` when migrations or schema docs change.
- Reload the schema cache and rerun affected contract checks when exposed SQL or PostgREST surfaces change.
