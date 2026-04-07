# Other Projects — Task 26 (Instagram Shared-Profile Rollout Guardrails)

Repo: TRR-Backend
Last updated: 2026-04-02

## Cross-Repo Snapshot
- TRR-Backend: Implemented in code and docs. Awaiting live deploy and authenticated canary.
- TRR-APP: Matching UI and proxy changes landed in TRR-APP TASK25.
- screenalytics: Not touched. No known contract consumer changes required in this task.

## Responsibility Alignment
- TRR-Backend
  - Shared-profile scraper/runtime hardening
  - Worker-health and catalog-progress alert contracts
  - Modal readiness verification and worker-plane deploy
- TRR-APP
  - Admin shared-profile labels, alert rendering, and Bravo-alias copy cleanup
- screenalytics
  - No ownership in this task unless a downstream contract consumer is later identified

## Dependency Order
1. TRR-Backend backend/runtime changes and docs
2. TRR-APP admin UI alignment
3. Modal deploy and authenticated canary/replay

## Locked Contracts (Mirrored)
- Social admin route topology remains stable.
- `account_handle` stays the durable route key.
- `network_name` is additive metadata for operator-facing labels and templates.
