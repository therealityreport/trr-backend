# TRR Stack Audit Remediation — Task 9 Plan

Repo: TRR-Backend
Last updated: February 17, 2026

## Goal

Execute backend-owned remediation for stack drift, env contract normalization, and dependency install hardening.

## Scope

1. Canonicalize Gemini model env contract to `GEMINI_MODEL` with deprecated fallback support for `GEMINI-MODEL`.
2. Add env contract checks for `.env.example` (duplicate keys, invalid key names with migration allowlist, required key presence).
3. Add repository conflict-marker CI guard.
4. Prepare Python install flow for lock-driven execution while keeping existing install compatibility.
5. Keep cross-repo sequencing authoritative: Backend -> screenalytics -> TRR-APP.

## Out of Scope

- Breaking API response shape changes.
- TRR-APP UI implementation details.

## Locked Contracts

- `TRR_INTERNAL_ADMIN_SHARED_SECRET` remains shared with TRR-APP.
- `SCREENALYTICS_SERVICE_TOKEN` remains required for `/api/v1/screenalytics/*`.
- Gemini env canonical key is `GEMINI_MODEL`; `GEMINI-MODEL` is temporary fallback only.

## Acceptance Criteria

1. Backend CI includes merge-conflict marker guard.
2. Backend Gemini model resolution prefers canonical key and logs fallback usage.
3. Backend env checks fail on duplicate keys and unexpected hyphenated keys.
4. TASK9 docs are synchronized with screenalytics TASK7 and TRR-APP TASK8.
