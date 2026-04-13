# AGENTS — TRR-Backend

Last reviewed: 2026-04-09

Read `../AGENTS.md` first. That workspace file is the canonical policy; use this file only for backend-local rules.

## Scope
- `api/` — FastAPI entrypoints, routers, request wiring, and API surface ownership
- `trr_backend/` — domain logic, integrations, repositories, clients, and shared backend services
- `supabase/` — migrations, schema docs, and database-facing contracts owned by this repo
- `scripts/` — backend developer tooling, maintenance scripts, and operational helpers

## Non-Negotiable Rules
- Backend lands API, schema, and exposed SQL changes before downstream TRR-APP follow-through.
- Current API prefix is `/api/v1`. Breaking changes require a new version prefix rather than silent contract drift.
- Prefer additive API changes. If a response shape changes, update affected consumers in the same session.
- Never edit an existing migration. Add a new migration, keep schema docs current, and run `./scripts/reload_postgrest_schema.sh` when PostgREST or exposed SQL surfaces change.
- Keep backend-owned people-count and image-analysis contracts current in `trr_backend/vision/people_count_service.py`.
- Preserve `ADMIN_EMAIL_ALLOWLIST` and `TRR_INTERNAL_ADMIN_SHARED_SECRET`.

## Supabase Conventions
- Migrations are additive and never rewritten after landing.
- Keep row-level security policies explicit alongside schema changes when tables or access rules depend on them.
- Keep schema docs current whenever migrations change the exposed database shape.
- Do not add edge-function obligations to this repo policy unless `supabase/functions/` is introduced into the repo.

## Local Dev
- Use `docs/README_local.md` for backend-local setup and run instructions.

## Validation
- `ruff check .`
- `ruff format --check .`
- `pytest -q`
- `make schema-docs-check` when migrations or schema docs change

Use `../scripts/handoff-lifecycle.sh pre-plan`, `post-phase`, and `closeout` exactly as required by `../AGENTS.md`. Update canonical status sources, not generated `docs/ai/HANDOFF.md`.
