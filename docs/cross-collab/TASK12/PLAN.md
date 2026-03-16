# Cast Screen-Time Analytics — Task 12 Plan

Repo: TRR-Backend
Last updated: 2026-03-16

## Goal
Cast Screen-Time Analytics

## Status Snapshot
Backend now supports persisted operator decision state, title-card reference publish ingestion, a deployed smoke runner, a stale-run drill script, and cutover/rollback tooling for cast-screentime closure across P0-P3, while live evidence capture remains operational follow-up.

## Scope

### Phase 7: Closure Tooling
Close the remaining backend-owned cast-screentime gaps by adding executable deployed smoke tooling and explicit cutover/rollback guidance for the original zero-trust program.

Files to change:
- `scripts/ops/cast_screentime_deployed_smoke.py`
- `scripts/ops/cast_screentime_stale_run_drill.py`
- `docs/cross-collab/TASK12/DEPLOYED_VALIDATION_RUNBOOK.md`
- `docs/cross-collab/TASK12/CUTOVER_CHECKLIST.md`
- `docs/cross-collab/TASK12/ACCEPTANCE_REPORT.md`
- `docs/cross-collab/TASK12/PLAN.md`
- `docs/cross-collab/TASK12/OTHER_PROJECTS.md`
- `docs/cross-collab/TASK12/STATUS.md`
- `docs/ai/HANDOFF.md`

Delivered behavior:
- add a repeatable deployed smoke runner for upload/import/run/approve/publish paths
- add a repeatable stale-run drill so reconciliation can be validated without manual SQL assembly
- document the deployed validation steps needed to close the remaining `P0-P3` operational gates
- document the feature-flag-based cutover and rollback steps for the `TRR-APP` admin surface

## Out of Scope
- Items owned by other repos unless explicitly required.

## Locked Contracts
- Keep shared API/schema contracts synchronized across affected repos.
- Public admin routes live under `/api/v1/admin/cast-screentime/*`.
- Worker-only routes live under `/api/v1/internal/screenalytics/cast-screentime/*`.
- `runs_v2.run_type='cast_screentime'` is mandatory for the new lane.

## Acceptance Criteria
1. TRR-Backend changes complete and validated.
2. Cross-repo dependency order is respected.
3. Fast checks pass for TRR-Backend.
4. Task docs remain synchronized.
