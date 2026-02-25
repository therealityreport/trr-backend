# AGENTS — TRR-Backend (Operational Rules)

This file is the canonical policy for agents working in `TRR-Backend`.
Workspace-level coordination rules are defined at:
- `/Users/thomashulihan/Projects/TRR/AGENTS.md`
- `/Users/thomashulihan/Projects/TRR/CLAUDE.md`

## Scope
- FastAPI app: `api/` (mounted under `/api/v1/*`)
- Shared backend library: `trr_backend/`
- Supabase schema/migrations: `supabase/`
- Operational scripts: `scripts/`

## Git Workflow
- Default: work on `main` unless explicitly asked otherwise.
- Only create/use a branch or worktree if explicitly requested.
- Never force-push to `main`.

## Start-of-Session Checklist
1. Read this file and `/Users/thomashulihan/Projects/TRR/TRR-Backend/CLAUDE.md`.
2. Read workspace `/Users/thomashulihan/Projects/TRR/AGENTS.md` for cross-repo ordering.
3. Confirm whether task changes API contracts, DB schema, or screenalytics integration.

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

Medium checks (when relevant):
```bash
make schema-docs-check
make repo-map-check
```

Slow/CI simulation:
```bash
make ci-local
```

## Coding and API Conventions
- Python 3.11+, explicit types on public functions, no bare `except`.
- `ruff` is formatting/lint source of truth.
- FastAPI routers live in `api/routers/`.
- Prefer additive API changes; preserve backward compatibility.

Contract rules:
- If TRR-APP consumers are affected, update in same session:
  - `TRR-APP/apps/web/src/lib/server/trr-api/`
- If screenalytics integration changes, update:
  - `trr_backend/clients/screenalytics.py`

## Auth and Admin Safety
- Admin access must be allowlist-only (`ADMIN_EMAIL_ALLOWLIST`).
- Shared secret with TRR-APP: `TRR_INTERNAL_ADMIN_SHARED_SECRET`.
- Service token for `/api/v1/screenalytics/*`: `SCREENALYTICS_SERVICE_TOKEN`.

## Database and Migrations
- Never edit an existing migration; create a new migration.
- Keep schema docs in sync after migration/schema changes.
- If PostgREST cannot see new functions, reload schema cache:
```bash
./scripts/reload_postgrest_schema.sh
```

## Cross-Repo Implementation Order (Must Follow)
Implementation order is fixed by workspace policy:
1. `TRR-Backend` schema/contracts first
2. `screenalytics` consumers next (if impacted)
3. `TRR-APP` UI/consumer updates last

## Validation and Handoff (Required)
Before ending session:
1. Run fast checks for changed backend behavior.
2. Run targeted tests for changed endpoints/contracts.
3. Update `docs/ai/HANDOFF.md`.

## Skill Routing (Repo)
Use the smallest set of skills that fully covers the backend task after the mandatory default skill chain is applied.

## Default Skill Chain (Mandatory)
Apply this chain for all non-trivial implementation tasks (any task that changes repo-tracked files, validates behavior for a change, or prepares commit/PR/handoff artifacts):
1. `orchestrate-plan-execution`
2. `senior-fullstack`
3. `senior-backend` or `senior-frontend` (pick by primary surface)
4. `senior-qa`
5. `code-reviewer`

Backend mapping for step 3:
- Default to `senior-backend` for this repo.
- Use `senior-frontend` only when the task is primarily UI/rendering/interaction and has no backend/schema/API/pipeline contract risk.
- Tie-breaker when both surfaces are touched: choose `senior-backend` if any contract/schema/pipeline semantics are changed; otherwise choose `senior-frontend`.

Exceptions:
- Trivial read-only tasks and simple Q&A.
- Explicit user override.
- Skill unavailable (must document fallback).

Domain skills are additive:
- `senior-architect`, `senior-devops`, `tdd-guide`, and other domain skills may be added, but the five baseline steps must not be skipped when the trigger rule applies.

Handoff compliance keys for qualifying tasks (`docs/ai/HANDOFF.md`):
- `default_skill_chain_applied` (`true|false`)
- `default_skill_chain_used` (ordered list)
- `default_skill_chain_exception_reason` (required when not applied)

Primary skills:
- `orchestrate-plan-execution`: Codex-primary default entrypoint for non-trivial plan + execute tasks; Claude remains secondary.
- `senior-backend`: API routes, schema/migrations, backend performance/security.
- `senior-architect`: system/API design tradeoffs, layering/dependency checks.
- `senior-qa`: backend test coverage and regression validation.
- `code-reviewer`: PR risk scans and prioritized review findings.

Secondary skills:
- `senior-fullstack`: if backend change requires coordinated frontend integration.
- `tdd-guide`: test-first implementation workflow.
- `senior-devops`: CI/deploy/pipeline hardening.
- `tech-stack-evaluator`: only for non-trivial architecture/tooling decisions.
- `aws-solution-architect`: only if AWS migration/architecture is explicitly requested.

Skill sequencing for backend feature work (after default chain):
1. `senior-architect` (if decision-heavy)
2. `senior-backend`
3. `senior-qa`
4. `code-reviewer` (review/refinement)
