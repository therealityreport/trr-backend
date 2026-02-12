# Other Projects — Task 7 (Bravo Import + Cast Eligibility + Videos/News)

Repo: TRR-Backend
Last updated: February 11, 2026

## Cross-Repo Snapshot

- TRR-Backend: Implemented Bravo parser/endpoints/snapshot persistence (this task).
- TRR-APP: Add proxy routes + show/season/person tabs + cast eligibility defaults (TASK6).
- screenalytics: No code changes expected (TASK6 status only).

## Dependency Order

1. TRR-Backend (this repo): Bravo source + parser + persisted APIs.
2. TRR-APP: wire admin proxy and UI against persisted Bravo endpoints.
3. screenalytics: only check for drift if dependency changes.

## Locked Contracts

- Bravo reads are DB-backed from source snapshots; no live fetch on UI render.
- Person social merge policy is fill-missing-only.
- Show/person source snapshots use `source_id='bravo'`, `variant='default'`.
