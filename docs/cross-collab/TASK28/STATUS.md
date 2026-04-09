# Status — Task 28 (Concerns remediation and Screenalytics contract lock)

Repo: TRR-Backend
Last updated: 2026-04-09

## Handoff Snapshot
```yaml
handoff:
  include: true
  state: active
  last_updated: 2026-04-09
  current_phase: "backend contract cutover landed"
  next_action: "continue backend decomposition and remove remaining Screenalytics compatibility surfaces"
  detail: self
```

## Phase Status

| Phase | Description | Status | Notes |
|---|---|---|---|
| 1 | Contract lock + backend-first cutover | In Progress | Cursor gallery contract landed; Screenalytics-facing auth now defaults to internal-admin JWTs. |
| 2 | Backend-owned migration follow-through | Pending | Update transitional callers after backend contracts land. |

## Blockers
- None.

## Recent Activity
- 2026-04-09: Task scaffolding created.
- 2026-04-09: Locked the retirement direction for `screenalytics`; backend is the producer-side contract owner for auth and gallery pagination changes.
- 2026-04-09: Added cursor-aware show/season asset pagination metadata and backend tests for cursor decoding plus truthful `full=true` truncation.
- 2026-04-09: Changed `api/screenalytics_auth.py` so service-token fallback is opt-in instead of default.
- 2026-04-09: Verification:
  - targeted `pytest -q tests/api/test_admin_show_reads.py` passed
  - targeted `ruff check` and `ruff format --check` for touched backend files passed
  - full `ruff check .` / `ruff format --check .` still fail on unrelated pre-existing workspace issues outside this task
