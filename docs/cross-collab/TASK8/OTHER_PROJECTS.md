# Other Projects — Task 8 (Season Social Analytics V3)

Repo: TRR-Backend
Last updated: February 17, 2026

## Cross-Repo Snapshot

- TRR-Backend: Implemented analytics contract + contextual sentiment upgrades, then expanded Bravo scoped account targeting (including WWHL and BravoDailyDish) for RHOSLC S6.
- TRR-APP: Consumes `weekly_platform_engagement`, updates weekly trend + sentiment-driver messaging, and now passes/retains `source_scope` into week-detail views while adding Bravo/Creator social dashboard shells.
- screenalytics: No changes required for this feature.

## Dependency Order

1. TRR-Backend first (additive contract and sentiment logic).
2. TRR-APP second (chart rendering + UI copy updates).
3. screenalytics unchanged unless future contract usage is introduced.

## Locked Contracts

- Existing fields (`weekly`, `weekly_platform_posts`, `themes`) remain intact.
- New field is additive only: `weekly_platform_engagement`.
- Gemini disambiguation remains optional and off by default.
