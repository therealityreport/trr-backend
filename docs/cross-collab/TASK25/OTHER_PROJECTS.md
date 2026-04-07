# Other Projects — Task 25 (TRR-APP Supabase Simplification and Index Hardening)

Repo: TRR-Backend
Last updated: 2026-04-02

## Cross-Repo Snapshot
- TRR-Backend: Owns the migration, live advisor verification, and schema-level duplicate-index cleanup.
- TRR-APP: Must follow after the migration by disabling Flashback gameplay routes, removing browser Supabase data access, and cleaning app env/docs/tests.
- screenalytics: No repo edits planned in this task, but `screenalytics.*` tables are part of the backend migration index batch.

## Responsibility Alignment
- TRR-Backend
  - Add the new migration and validate the targeted advisor findings are resolved.
  - Verify the hot `core.season_images` query plan improves after the migration.
- TRR-APP
  - Remove the Flashback gameplay entry points and browser Supabase data path after backend work completes.
  - Keep only the server-side Supabase auth verifier usage of `@supabase/supabase-js`.
- screenalytics
  - No direct code change in this session unless validation exposes a migration-side compatibility issue.

## Dependency Order
1. TRR-Backend
2. screenalytics
3. TRR-APP

## Locked Contracts (Mirrored)
- Performance cleanup scope is limited to the approved advisor batch.
- App runtime Postgres and server-side Supabase auth contracts stay unchanged while this repo lands the DB work.
