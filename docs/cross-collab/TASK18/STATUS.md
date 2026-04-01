# Status — Task 18 (Social backfill remediation for Instagram and TikTok)

Repo: TRR-Backend
Last updated: 2026-03-30

## Handoff Snapshot
```yaml
handoff:
  include: true
  state: recent
  last_updated: 2026-03-30
  current_phase: "Validation and closeout"
  next_action: "Resolve unrelated schema-doc drift or accept targeted validation for this remediation-only change set"
  detail: self
```

## Phase Status

| Phase | Description | Status | Notes |
|---|---|---|---|
| 0 | Repository contracts and shared helpers | Complete | Added Instagram metadata retry-state helpers, TikTok canonical URL/saves helpers, resume and inline-worker fixes, and repository tests. |
| 1 | Instagram backfill scripts | Complete | Metadata/media and reel-view scripts updated with ordering, retry, auth-preflight, and degraded diagnostics coverage. |
| 2 | TikTok backfill scripts | Complete | Saves backfill now requires explicit season targeting, filters candidates, uses shared parsing, and retries canonical URLs. |
| 3 | Shared orchestration and API wiring | Complete | Inline worker registration, advisory-lock start guard, resumable frontier seeding, TikTok preview auth preflight, and regression tests landed. |
| 4 | Validation and closeout | Blocked | Targeted lint and pytest passed. `make schema-docs-check` reports unrelated existing schema-doc drift across core tables. |

## Blockers
- `make schema-docs-check` fails because the connected database regenerates broad existing drift in `supabase/schema_docs/` unrelated to Task 18. That drift was not folded into this backfill remediation change set.

## Recent Activity
- 2026-03-30: Created TASK18 scaffolding and ran `../scripts/handoff-lifecycle.sh pre-plan`.
- 2026-03-30: Implemented repository-layer Instagram retry-state handling, TikTok canonical URL/saves helpers, inline worker registration, resumable frontier seeding, and concurrent catalog-start protection.
- 2026-03-30: Landed Instagram and TikTok backfill script fixes with new targeted tests, using subagents for Instagram and TikTok script slices.
- 2026-03-30: Verified remediation files with `ruff check` and `ruff format --check`, and ran targeted pytest: `30 passed`.
- 2026-03-30: Ran `make schema-docs-check`; it failed on unrelated pre-existing schema-doc drift in `supabase/schema_docs/`.
