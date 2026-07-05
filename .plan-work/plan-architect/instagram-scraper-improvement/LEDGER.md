# LEDGER.md

## BUGS

| ID | Severity | Finding | Evidence | Required Fix | Validation |
|---|---|---|---|---|---|
| BUG-001 | High | No single benchmark currently proves Complete Instagram Post Snapshot speed and gaps at the account/date-window level. | `benchmark_posts_backfill.py` is payload-only; PRD requires account runtime, p95 timing, pressure, and gaps. | Build baseline benchmark/gap report around existing run/progress/queue read models. | Fixture tests plus guarded read-only live mode; output contains runtime, completeness, retries, Supabase pressure, Instagram risk. |
| BUG-002 | High | Completion can be misread unless missing snapshot parts become explicit retry targets. | PRD requires post detail, media, comments, replies, comment media, and avatars to be captured or source-unavailable. | Add/extend completion and retry-target records. | Tests prove partial success, retryable gaps, and source-unavailable evidence are distinct. |
| BUG-003 | High | Speed controls are currently lane-local or implicit, not a shared Adaptive Scrape Control Plane. | ADR accepts shared controller with lane-specific enforcement. | Add shared budget decision module and persistence/cache seam. | Budget precedence and pressure tests pass; lanes consume budgets. |
| BUG-004 | Medium | Hosted media completion is not yet the universal completion gate. | PRD states source URLs alone are partial; comment persistence and media guards exist but need unified reporting. | Add hosted snapshot completion reporting and retry targets. | Media/avatar/comment-media tests distinguish source URL partial from hosted complete/unavailable. |

## SUGGESTIONS

| ID | Type | Existing Scope | Suggested Change | Why | Acceptance Criteria |
|---|---|---|---|---|---|
| SUG-001 | Sequencing | Benchmark helpers and progress scripts | Start with baseline benchmark/gap report before tuning. | Prevents optimizing from assumptions. | First implementation issue produces report against fixtures and guarded live reads. |
| SUG-002 | Safety | Control plane and lane workers | Use conservative defaults for unknown thresholds, then permit benchmark-scoped ramping only. | Protects Supabase and Instagram identities. | Permanent defaults require benchmark evidence and explicit config changes. |
| SUG-003 | Operations | Modal readiness and runbooks | Add Modal validation to every scraper/runtime code issue. | Local-only success is insufficient for Modal-deployed lanes. | Handoff requires Modal readiness/deploy status for runtime changes. |

## RESTRUCTURE

| ID | Current Shape | New Shape | Dependency Impact | Ownership Impact | Validation Impact |
|---|---|---|---|---|---|
| RES-001 | Pressure/backoff behavior is distributed across lane-specific logic and operator scripts. | Shared control-plane budget decision service plus lane-specific enforcement. | Requires new/extended control-plane module and lane budget reads. | Control plane owns decisions; lanes own enforcement. | Budget tests plus lane integration tests. |
| RES-002 | Completeness is inferred from separate post/comment/media outputs. | Explicit snapshot-part completion and retry targets. | May require additive schema or metadata extensions. | Persistence/control plane own state; lanes report parts. | Completion contract tests and progress/API report tests. |
