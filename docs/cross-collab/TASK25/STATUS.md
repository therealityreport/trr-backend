# Status — Task 25 (TRR-APP Supabase Simplification and Index Hardening)

Repo: TRR-Backend
Last updated: 2026-04-02

## Handoff Snapshot
```yaml
handoff:
  include: true
  state: recent
  last_updated: 2026-04-02
  current_phase: "backend migration complete"
  next_action: "keep the generated schema docs with the migration and carry the recorded verification caveats forward"
  detail: self
```

## Phase Status

| Phase | Description | Status | Notes |
|---|---|---|---|
| 1 | Migration design and live advisor audit | Complete | Confirmed the exact missing FK indexes, duplicate indexes, and the hot `core.season_images` plan problem |
| 2 | Backend migration implementation | Complete | Added `20260402213000_supabase_connection_index_hardening.sql` and applied the same DDL through Supabase |
| 3 | Validation and live advisor recheck | Complete | Live `EXPLAIN` now uses `core_season_images_show_season_hosted_idx`; targeted duplicate indexes are gone |

## Blockers
- No implementation blockers.
- Repo-wide Python validation still has pre-existing drift outside this task: `ruff check .` fails on an unrelated long line in `tests/api/routers/test_admin_scrape_contracts.py`, and `ruff format --check .` reports unrelated formatting drift in existing files.
- `make schema-docs-check` generated the expected `supabase/schema_docs/*` updates for the new indexes and duplicate-index removals; those generated docs are now part of the task output.

## Recent Activity
- 2026-04-02: Task scaffolding created.
- 2026-04-02: Confirmed the app-side runtime Postgres lane is already correct and narrowed backend work to performance-advisor index hardening.
- 2026-04-02: Audited live `pg_indexes`, `pg_constraint`, `pg_stat_statements`, and performance advisors to build the final migration input list.
- 2026-04-02: Applied the index-hardening migration through Supabase and verified the hot `core.season_images` query switched to `core_season_images_show_season_hosted_idx`.
- 2026-04-02: Regenerated backend schema docs and synced handoffs through `post-phase` and `closeout`.
