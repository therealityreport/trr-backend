# AGENTS — TRR-Backend

Read `../AGENTS.md` first. That workspace file is the canonical policy; use this file only for backend-local rules.

## Scope
- `api/`
- `trr_backend/`
- `supabase/`
- `scripts/`

## Non-Negotiable Rules
- Backend lands API, schema, and exposed SQL changes before downstream screenalytics or TRR-APP follow-through.
- Prefer additive API changes. If a response shape changes, update affected consumers in the same session.
- Never edit an existing migration. Add a new migration, keep schema docs current, and run `./scripts/reload_postgrest_schema.sh` when PostgREST or exposed SQL surfaces change.
- Keep screenalytics-facing contracts current in `trr_backend/clients/screenalytics.py`.
- Preserve `ADMIN_EMAIL_ALLOWLIST`, `TRR_INTERNAL_ADMIN_SHARED_SECRET`, and `SCREENALYTICS_SERVICE_TOKEN`.

## Validation
- `ruff check .`
- `ruff format --check .`
- `pytest`
- `make schema-docs-check` when migrations or schema docs change

Use `../scripts/handoff-lifecycle.sh pre-plan`, `post-phase`, and `closeout` exactly as required by `../AGENTS.md`. Update canonical status sources, not generated `docs/ai/HANDOFF.md`.
