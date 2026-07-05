# IMPROVEMENTS.md

| ID | Category | Decision | Improvement | Why | Acceptance Criteria | Revised Plan Mapping |
| --- | --- | --- | --- | --- | --- | --- |
| IMP-001 | Verification and proof | accepted | Add a deterministic fixture benchmark report before live runs. | Gives repeatable proof for CI/local validation. | Fixture report validates required keys and safe defaults. | Phase 1 |
| IMP-002 | Runtime, cache, deploy, or environment fit | accepted | Gate live benchmark mode behind explicit confirm and active-job preflight. | Prevents accidental competing Instagram jobs. | Live mode refuses to run without account, confirmation, and preflight. | Phase 1 |
| IMP-003 | Data, migration, or config safety | accepted | Use additive schema/metadata for completion and retry targets. | Avoids breaking existing rows and readers. | Existing tests pass; new fields are nullable/backfillable. | Phase 2 |
| IMP-004 | Observability and operations | accepted | Include budget cause, pressure evidence, and next retry action in operator output. | Makes slowdowns and gaps explainable. | Progress/report output shows budget state and gap action. | Phases 3, 7 |
| IMP-005 | Scope control and safety | accepted | Keep Apify and external Instagram packages out of implementation. | Current contracts require TRR-native source families. | No new Apify/Instagram client dependency is introduced. | All phases |
| IMP-006 | Handoff and ownership | accepted | Split implementation into ordered issues/subagent scopes. | Reduces risk in a dirty nested repo. | Handoff names serialized phases and validations. | Handoff |
| IMP-007 | Docs freshness and external assumptions | deferred | Use Context7 for Scrapling/Patchright only if implementation changes their APIs. | Current plan relies on local source and pinned docs, not new external API syntax. | If API usage changes, execution agent resolves current docs before editing. | Validation |
| IMP-008 | User-facing output or UX quality | deferred | Add UI polish to display completion budgets. | Useful but not required for backend-first plan. | Future app issue after backend API shape stabilizes. | Out of scope |
