---
name: senior-backend
description: Repo-local canonical owner for TRR-Backend FastAPI endpoints, schema changes, contracts, persistence semantics, performance, and security-sensitive backend behavior.
---
Use this repo-local skill for implementation inside `TRR-Backend` when backend contracts, schema, or persistence behavior are changing.

## When to use
1. FastAPI routes, services, repositories, or schema/migrations are changing.
2. Backend performance, auth/authz, validation, or persistence semantics matter.
3. A backend change can affect `screenalytics` or `TRR-APP`.

## When not to use
1. Frontend-only or pure UI work.
2. Docs-only or review-only requests.
3. Generic FastAPI tutorial guidance.

## Preflight
1. Identify the contract owner and affected consumers.
2. Identify whether the change touches:
   - request/response schema
   - DB schema or migration
   - auth/authz
   - async or concurrency behavior
   - integration clients
3. Confirm same-session downstream updates if contracts drift.

## Backend checklist
1. Prefer additive contract changes where possible.
2. Keep migration discipline: never rewrite existing migrations.
3. Apply FastAPI/Pydantic/async checks:
   - model validation
   - response typing
   - async boundaries
   - dependency wiring
4. Apply security checks:
   - auth/authz correctness
   - input validation
   - secret handling
   - privilege boundaries
5. Update affected consumers in the same session when contracts or semantics change.

## Imported strengths
1. From `fastapi-expert`: FastAPI/Pydantic/async implementation checks.
2. From `secure-code-guardian`: auth/authz, input validation, and security-control prompts.

## Explicit rejections
1. Do not add generic framework tutorial content.
2. Do not import non-TRR API conventions that conflict with repo contracts.

## Completion contract
Return:
1. `backend_surface`
2. `contracts_changed`
3. `migrations_added`
4. `downstream_updates`
5. `validation_run`
