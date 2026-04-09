# Other Projects — Task 28 (Concerns remediation and Screenalytics contract lock)

Repo: TRR-Backend
Last updated: 2026-04-09

## Cross-Repo Snapshot
- TRR-Backend: Active contract owner. Backend cuts auth and gallery contracts first in Task 28.
- TRR-APP: Pending Task 26 adoption of backend gallery cursor responses and brittle-test rewrites.
- screenalytics: Pending Task 14 transitional cleanup so retirement continues without dual-auth defaults or startup side effects.

## Responsibility Alignment
- TRR-Backend
  - Own the retirement decision for backend-facing contracts and the producer-side gallery API changes.
- TRR-APP
  - Consume backend gallery cursor contracts and replace source-shape tests with behavior-focused coverage.
- screenalytics
  - Shrink transitional runtime assumptions, prefer internal-admin auth, and gate startup cleanup while retirement continues.

## Dependency Order
1. TRR-Backend
2. screenalytics
3. TRR-APP

## Locked Contracts (Mirrored)
- `TRR_INTERNAL_ADMIN_SHARED_SECRET` is the canonical auth for surviving Screenalytics-facing backend routes.
- `SCREENALYTICS_SERVICE_TOKEN` is transition-only and opt-in.
- Gallery reads move from capped offset loops to cursor pagination with downstream adoption in the same task.
