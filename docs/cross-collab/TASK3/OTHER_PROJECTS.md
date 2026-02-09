# Other Projects — Task 3 (Screenalytics Data Layer Unification)

Repo: TRR-Backend
Last updated: February 8, 2026

## Cross-Repo Snapshot

- TRR-Backend: Not yet started. Owns Phase 1 (migrations) and Phase 3 (integration file verification).
- screenalytics: Not yet started. Owns Phases 2-5 (code changes, data migration, Docker removal). See screenalytics TASK4.
- TRR-APP: No changes required for this task.

## Responsibility Alignment

- TRR-Backend
  - Owns Supabase migrations 0102-0105 (new screenalytics tables).
  - Owns Phase 1 preflight (schema drift detection).
  - Verifies integration files match resolved v2 column names.
- screenalytics
  - Owns all code changes: DB connection unification (Phase 2a), entity table removal (2b), ID mapping removal (2c), JSON store reduction (2d), schema-prefix queries (2e).
  - Owns v2 stub wiring (Phase 3 primary), data migration script (4a), Docker Postgres removal (4b).
  - Owns manifest cleanup (Phase 5).
- TRR-APP
  - No work required. Zero code changes for Phases 1-5.

## Dependency Order

1. TRR-Backend applies migrations 0102-0105 to staging Supabase.
2. TRR-Backend verifies integration files (Phase 3 partial).
3. screenalytics begins Phase 2 (code changes depend on migrations existing in Supabase).
4. screenalytics completes Phases 2-3, then runs Phase 4a (data migration) on staging.
5. After Phase 4a verification, screenalytics executes Phase 4b (remove Docker Postgres).
6. Mark task complete across repos.

## Locked Contracts (Mirrored)

- `TRR_DB_URL` as canonical connection env var (both repos).
- `screenalytics.*` schema prefix for all table queries.
- v2 table column names per TRR-Backend migration 0093 (`id` as PK, `manifest_key` not `s3_manifest_key`).
- RLS: service_role only for all `screenalytics.*` tables.
