# AGENTS — TRR-Backend (Operational Rules)

This file defines how automated agents and contributors should work in **TRR-Backend**.
For cross-repo coordination rules, see the workspace root:
- `/Users/thomashulihan/Projects/TRR/AGENTS.md`
- `/Users/thomashulihan/Projects/TRR/CLAUDE.md`

## Scope
- API: FastAPI app in `api/` (routes mounted under `/api/v1`)
- Library: shared code in `trr_backend/`
- Database: Supabase migrations and schema artifacts in `supabase/`
- Scripts: sync/backfill/verify utilities in `scripts/`

## Git Workflow
This repo intentionally does **not** prescribe branching.
- Default is work on `main`.
- Only create/use a branch or worktree if explicitly asked.
- Never force-push to `main`.

## Essential Commands
Setup:
```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Run API (dev):
```bash
./start-api.sh
```

Fast checks (pre-commit):
```bash
ruff check .
ruff format --check .
pytest
```

Medium checks (pre-PR / when relevant):
```bash
make schema-docs-check       # if schema/migrations changed
make repo-map-check          # if repo structure changed
```

Slow (CI simulation):
```bash
make ci-local
```

## Coding Conventions
- Python: 3.11+, type hints on public functions, explicit exceptions (no bare `except`).
- Formatting/linting: `ruff` is the source of truth.
- FastAPI:
  - Routers live in `api/routers/`
  - Prefer small request/response models and keep backwards compatibility.

## API Contract Rules
- Additive changes only unless you update all consumers in the same change set.
- New response fields must be optional for consumers (and safely defaulted).
- If you change any endpoint shape used by TRR-APP, update the consumer code:
  - `TRR-APP/apps/web/src/lib/server/trr-api/`
- If you change any screenalytics integration behavior, update:
  - `trr_backend/clients/screenalytics.py`

## Auth / Admin Safety
- Admin access must be allowlist-only (`ADMIN_EMAIL_ALLOWLIST`).
- Shared secret between TRR-APP and TRR-Backend:
  - `TRR_INTERNAL_ADMIN_SHARED_SECRET`
- Service-to-service token for `/api/v1/screenalytics/*`:
  - `SCREENALYTICS_SERVICE_TOKEN`

## Database / Migrations
- Never edit an existing migration. Always create a new migration.
- After changing migrations/schema, keep docs in sync:
  - `make schema-docs-check` (or `make schema-docs-reset-check` for a fresh DB)

## PostgREST Schema Cache
If you add/modify database functions and PostgREST can’t see them:
```bash
./scripts/reload_postgrest_schema.sh
```

## Cross-Repo Coordination (TRR Workspace)
- Implementation order: TRR-Backend first → screenalytics (if impacted) → TRR-APP.
- Keep `docs/cross-collab/TASK*/` aligned with any cross-repo changes.
- Update `docs/ai/HANDOFF.md` before ending a session if you touched this repo.

