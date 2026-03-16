# Other Projects — Task 12 (Cast Screen-Time Analytics)

Repo: TRR-Backend
Last updated: 2026-03-16

## Cross-Repo Snapshot
- TRR-Backend: canonical publish/version history now also carries persisted suggestion and unknown-review decision state, plus a deployed smoke runner and cutover checklist for live closure
- TRR-APP: admin page now exposes operator actions for suggestions and unknown queues, plus feature-flag rollback gating while preserving promo assets as independent reports
- screenalytics: worker lane now emits deterministic title-card reference artifacts, stable queue ids, classifier-backed confessional metadata, executable Golden Dataset comparisons, and a formal reuse matrix

## Responsibility Alignment
- TRR-Backend
  - Owns upload verification, asset promotion, run creation, run reads, review-state writes, and worker-write endpoints.
- TRR-APP
  - Consumes only backend admin APIs through app-owned proxy routes.
- screenalytics
  - Reads the frozen run contract from DB and writes artifacts/state back through backend internal routes.

## Dependency Order
1. TRR-Backend
2. screenalytics
3. TRR-APP

## Locked Contracts (Mirrored)
- Keep shared contracts aligned with owning repo PLAN.md.
- Old screenalytics operator flows and legacy run tables remain out of scope for the new lane.
