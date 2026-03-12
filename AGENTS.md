# AGENTS — TRR-Backend (Canonical Repo Policy)

This file is the canonical policy for agents working in `TRR-Backend`.
`CLAUDE.md` in this repo is a pointer shim only.

Workspace coordination policy:
- `/Users/thomashulihan/Projects/TRR/AGENTS.md`

## Scope
- FastAPI app: `api/` (mounted under `/api/v1/*`)
- Shared backend library: `trr_backend/`
- Supabase schema/migrations: `supabase/`
- Operational scripts: `scripts/`

## Start-of-Session Checklist
1. Read this file first.
2. Read workspace policy: `/Users/thomashulihan/Projects/TRR/AGENTS.md`.
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

Fast checks:
```bash
ruff check .
ruff format --check .
pytest
```

Medium checks:
```bash
make schema-docs-check
make repo-map-check
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
- Admin access must remain allowlist-only (`ADMIN_EMAIL_ALLOWLIST`).
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
1. `TRR-Backend` schema/contracts first.
2. `screenalytics` consumers next (if impacted).
3. `TRR-APP` UI/consumer updates last.

## Validation and Handoff (Required)
Before ending session:
1. Run fast checks for changed backend behavior.
2. Run targeted tests for changed endpoints/contracts.
3. Update `docs/ai/HANDOFF.md`.

## Skill Routing
Use repo-local and workspace-local TRR skills first.
Primary skills:
- `orchestrate-plan-execution`
- `senior-backend`
- `senior-fullstack`
- `senior-qa`
- `code-reviewer`
- `senior-architect`

Secondary skills:
- `senior-devops`
- `tdd-guide`
- `tech-stack-evaluator`
- `aws-solution-architect`

Canonical owners:
- `senior-backend` -> `/Users/thomashulihan/Projects/TRR/TRR-Backend/skills/senior-backend/SKILL.md`
- `database-designer` -> `/Users/thomashulihan/Projects/TRR/TRR-Backend/skills/database-designer/SKILL.md`
- `senior-fullstack` -> `/Users/thomashulihan/Projects/TRR/skills/senior-fullstack/SKILL.md`
- `senior-qa` -> `/Users/thomashulihan/Projects/TRR/skills/senior-qa/SKILL.md`
- `code-reviewer` -> `/Users/thomashulihan/Projects/TRR/skills/code-reviewer/SKILL.md`
- `senior-architect` -> `/Users/thomashulihan/Projects/TRR/skills/senior-architect/SKILL.md`
- `senior-devops` -> `/Users/thomashulihan/Projects/TRR/skills/senior-devops/SKILL.md`

Use global `~/.codex/skills` only for skills that remain globally canonical in the governance matrix.

### Before Each Plan
Before producing any plan:
1. Review the skills available for this backend scope:
   - `TRR-Backend/skills/` first
   - `/Users/thomashulihan/Projects/TRR/skills/` second
   - globally canonical `~/.codex/skills` third
   - alias or specialist skills only if no canonical owner fits cleanly
2. Choose the minimum skill set needed.
3. State which skills will be used for:
   - plan writing
   - implementation
4. Invoke and follow those selected skills during plan creation and implementation routing.
5. If no repo-local skill fits, fall back to workspace-local, then globally canonical.

### AWS Deploy Rule
For deployable AWS/cloud-infra/backend implementation work:
1. Trigger this rule only when backend implementation changes require rollout of running AWS-backed services, workers, infra config, or backend behavior.
2. Do not trigger this rule for docs-only, tests-only, local-only, or non-deployed changes.
3. The selected implementation skills must include:
   - `senior-devops`
   - `aws-solution-architect`
4. Required checks must pass before deploy.
5. Implementation is not complete until the AWS deployment is executed successfully to the primary production target when rollout is required.
6. Handoff must record deploy evidence and post-deploy verification.

## MCP Invocation Matrix
| MCP Server | Invoke When |
|---|---|
| `chrome-devtools` | Browser-based backend/admin UX verification through managed Chrome. |
| `figma` | Backend task includes design-linked UI contract checks requiring Figma context. |
| `figma-desktop` | Local desktop Figma workflows only when enabled. |
| `github` | PR/issue metadata and remote repository context checks. |
| `supabase` | Schema, SQL, migration, and project-level Supabase operations. |
| `awslabs-core` | First step for AWS task intent understanding. |
| `awslabs-aws-api` | Executing concrete AWS operations and backend infrastructure checks. |
| `awslabs-aws-docs` | Verifying AWS behavior/spec details from official docs. |
| `awslabs-pricing` | Cost analysis for AWS architecture or backend runtime options. |
| `awslabs-cloudwatch` | Alarm/log/metric triage for backend incidents. |
| `awsknowledge` | Architecture tradeoff decisions and AWS service selection guidance. |
| `awsiac` | IaC best-practice validation for backend deployment artifacts. |

## Drift Prevention
- Canonical repo policy is this `AGENTS.md`.
- `CLAUDE.md` remains pointer-only.
- If conflict exists, this file wins.
