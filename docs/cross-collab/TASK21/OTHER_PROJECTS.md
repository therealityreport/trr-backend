# Other Projects — Task 21 (Follow-up validation and regression hardening)

Repo: TRR-Backend
Last updated: 2026-03-31

## Cross-Repo Snapshot
- TRR-Backend: Shared auth repair and lint cleanup implemented. Focused validation is green; full `pytest -q` is still in progress.
- TRR-APP: Proxy auth cleanup, week-detail regression hardening, and app validation are complete. See TRR-APP TASK20.
- screenalytics: Validation closure is complete with a passing full suite. See screenalytics TASK12.

## Responsibility Alignment
- TRR-Backend
  - Own shared auth semantics, JWT verification behavior, and repo-wide validation closure.
- TRR-APP
  - Own admin proxy auth wiring, week-detail regression tests, and frontend validation.
- screenalytics
  - Own cast-screentime consumer validation and worker/task bootstrap verification.

## Dependency Order
1. TRR-Backend
2. screenalytics
3. TRR-APP

## Locked Contracts (Mirrored)
- `service_role` remains valid for ordinary backend admin routes.
- Cast screentime remains stricter: `service_role` callers still need `X-TRR-Internal-Admin-Secret`.
- TRR-APP admin proxies use `TRR_INTERNAL_ADMIN_SHARED_SECRET`-signed internal admin bearer tokens.
