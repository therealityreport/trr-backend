# Other Projects — Task 10 (Social Admin Incremental Sync + Runs UX Hardening)

Repo: TRR-Backend
Last updated: February 17, 2026

## Cross-Repo Snapshot

- TRR-Backend: Complete. See TRR-Backend TASK10.
- screenalytics: Validation complete, no code changes required. See screenalytics TASK7.
- TRR-APP: Complete. See TRR-APP TASK9.

## Responsibility Alignment

- TRR-Backend
  - Additive migration + ingest contract extension.
  - Incremental reconciliation and conservative missing-mark semantics.
  - Backend test coverage for strategy/missing behavior.
- screenalytics
  - Compatibility validation only (no dependency on social admin ingest/runs contract changes).
- TRR-APP
  - Polling/completion correctness, run-label UX, sync-strategy control wiring.

## Dependency Order

1. TRR-Backend migration and contract/logic implementation.
2. screenalytics compatibility validation.
3. TRR-APP consumer and UX hardening.

## Locked Contracts (Mirrored)

- Ingest `sync_strategy` is additive (`incremental` default).
- Missing comments are retained and flagged (`is_missing`) instead of deleted.
- Existing analytics totals behavior is preserved.
