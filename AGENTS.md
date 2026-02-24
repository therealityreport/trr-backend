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
Use the smallest set of skills that fully covers the backend task.

Primary skills:
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

Skill sequencing for backend feature work:
1. `senior-architect` (if decision-heavy)
2. `senior-backend`
3. `senior-qa`
4. `code-reviewer` (review/refinement)

## MCP Routing Matrix (Required)
Use deterministic routing. Pick one primary path first, then fallback only if blocked.

| Task Type | Primary Tool | Fallback Tool | Required When | Forbidden When |
|---|---|---|---|---|
| API/schema/contract investigation | Local CLI (`rg`, `pytest`, `ruff`) | N/A | Any backend contract or migration work | Do not use external web lookup for repo-local facts |
| Endpoint behavior through UI/admin flows | Playwright MCP | Chrome DevTools MCP | Backend changes affect app/admin runtime behavior | Do not skip local backend tests after UI validation |
| External framework/spec verification | Web tool (primary sources only) | Local docs already in repo | Versioned library/standard details are uncertain | Do not use non-primary sources for technical decisions |
| Work tracking and issue updates | Linear MCP | `docs/ai/HANDOFF.md` status note if Linear unavailable | Multi-step work, release-risk work, or handoff-heavy tasks | Do not omit status artifact for high-risk work |

Deterministic examples:
1. Endpoint response contract change -> Local CLI + `senior-backend`; classify risk under contract-impact modes below.
2. Admin regression suspected from backend change -> Playwright MCP first, then targeted backend tests.
3. Cross-repo contract rollout tracking -> Linear MCP plus required handoff schema.

## Sub-Agent Delegation Contract (Required)
Single agent may hold multiple roles, but every role deliverable below is still required.

Delegation tuple (required):
- `role`
- `scope`
- `deliverable`
- `verification_command`
- `status` (`pending|in_progress|completed|blocked`)

Required roles:
- `API Contract Owner`: documents request/response and compatibility class.
- `Schema Owner`: documents migration impact and schema sync evidence.
- `Integration Owner`: documents downstream impact on screenalytics/TRR-APP consumers.
- `QA Owner`: documents executed tests and regression outcome.

Required handoff location:
- `docs/ai/HANDOFF.md` must include `delegation_map` with one entry per required role.

## Execution Modes and Risk Gates
Contract-impact mode is mandatory for backend changes:
- `no_contract`: internal-only change; no consumer shape impact.
- `additive_contract`: new optional fields/endpoints; backward compatible.
- `breaking_contract`: changed/removed required fields, behavior, or schema contract.

Required gates by mode:
1. `no_contract`
- Run targeted tests for touched behavior.
- Document why consumer updates are not required.
2. `additive_contract`
- Run targeted tests.
- Document consumer impact assessment.
- Update downstream consumers in same session when they rely on new fields.
3. `breaking_contract`
- Block completion unless same-session downstream updates are completed in impacted repos.
- Include explicit compatibility/rollback note in handoff.

Severity gate:
- P0/P1 issues block completion until reproduction, fix, and regression evidence are documented.

## Acceptance Evidence Schema
All backend sessions that modify behavior must record these keys in `docs/ai/HANDOFF.md`:
- `primary_skill`
- `supporting_skills`
- `mcp_tools_used`
- `delegation_map`
- `risk_class`
- `validation_evidence`
- `downstream_repos_impacted`

Schema requirements:
1. `mcp_tools_used` must list primary and fallback decisions when used.
2. `delegation_map` must contain all required roles from this file.
3. `validation_evidence` must include command(s) and pass/fail result.
4. `downstream_repos_impacted` must explicitly list `TRR-Backend`, `screenalytics`, `TRR-APP` as `yes/no`.

## Policy Compliance Checks (Mandatory)
Run from `TRR-Backend/` before handoff:
```bash
rg -n "^## MCP Routing Matrix \\(Required\\)$" AGENTS.md
rg -n "^## Sub-Agent Delegation Contract \\(Required\\)$" AGENTS.md
rg -n "^## Execution Modes and Risk Gates$" AGENTS.md
rg -n "^## Acceptance Evidence Schema$" AGENTS.md
rg -n "^## Policy Compliance Checks \\(Mandatory\\)$" AGENTS.md
rg -n "^## Escalation and Stop Conditions$" AGENTS.md
rg -n "primary_skill|supporting_skills|mcp_tools_used|delegation_map|risk_class|validation_evidence|downstream_repos_impacted" docs/ai/HANDOFF.md
```

Pass criteria:
1. All section-header checks return exactly one match.
2. Handoff schema key check returns at least one match for each key on behavior-changing sessions.
3. No P0/P1 issue is marked complete without validation evidence.

## Escalation and Stop Conditions
Stop and escalate immediately when any of the following are true:
1. `breaking_contract` is detected and downstream repo updates cannot be completed in-session.
2. Migration or schema behavior is uncertain and cannot be verified with local checks.
3. Required MCP/tool path is unavailable and no documented fallback can produce equivalent evidence.
4. Secret handling, auth, or service-token risk is detected.

Escalation packet must include:
1. Current mode/risk class.
2. Blocking condition.
3. Evidence gathered so far.
4. Proposed unblock options with impact.
