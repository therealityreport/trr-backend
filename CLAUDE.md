# TRR-Backend — Claude/Codex Playbook

Canonical repo rules: `/Users/thomashulihan/Projects/TRR/TRR-Backend/AGENTS.md`.
Workspace coordination rules: `/Users/thomashulihan/Projects/TRR/AGENTS.md`.

## Start-of-Session Checklist
1. Read this file and `/Users/thomashulihan/Projects/TRR/TRR-Backend/AGENTS.md`.
2. Read workspace `/Users/thomashulihan/Projects/TRR/AGENTS.md`.
3. Confirm whether task impacts API contracts, schema, or screenalytics integrations.

## Quickstart
```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
./start-api.sh
```

## Fast Validation
```bash
ruff check .
ruff format --check .
pytest
```

## Mandatory Workflow
1. Keep API changes additive unless all consumers are updated in same session.
2. If schema/migrations change, keep docs in sync and run schema checks.
3. Follow workspace order for cross-repo tasks:
   - `TRR-Backend` -> `screenalytics` -> `TRR-APP`
4. Update `docs/ai/HANDOFF.md` before ending session.

## Skill Activation (Repo)
- `senior-backend`: default for API/schema/backend logic work.
- `senior-architect`: design/contract decisions or dependency/layer analysis.
- `senior-qa`: backend testing and regression hardening.
- `code-reviewer`: review/risk scanning.
- `senior-fullstack`: only when backend changes require coordinated app updates.
- `senior-devops`: CI/deployment readiness.
- `tdd-guide`: test-first implementation.
- `tech-stack-evaluator`: major tooling/architecture decisions only.
- `aws-solution-architect`: AWS-specific work only.

## Key Paths
- API routers: `api/routers/`
- Backend clients/integrations: `trr_backend/clients/`
- DB migrations/schema: `supabase/`
- Operational scripts: `scripts/`

## Contract References
- TRR-APP consumer path: `TRR-APP/apps/web/src/lib/server/trr-api/`
- Shared secret: `TRR_INTERNAL_ADMIN_SHARED_SECRET`
- Service token: `SCREENALYTICS_SERVICE_TOKEN`

## Session Continuity
Before ending session:
1. Record status and test evidence in `docs/ai/HANDOFF.md`.
2. Note remaining risks or follow-up tasks for the next contributor.
