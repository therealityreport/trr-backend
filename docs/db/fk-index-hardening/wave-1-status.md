# Wave-1 FK Index Hardening Status

- generated_at: `2026-04-20T18:35:19Z`
- inventory_source: [`wave-1-inventory.yml`](./wave-1-inventory.yml)
- representative_query_checks: [`scripts/db/fk_index_wave1_explain.sql`](../../scripts/db/fk_index_wave1_explain.sql)
- connection_mode_used_for_inventory: `direct`
- resolved_inventory_host: `db.vwxfvzutyufrkhfgoeaa.supabase.co`

## Pre-flight Checks

- Inventory regenerated from the live database on `2026-04-20T18:35:19Z`.
- Inventory ran on `direct` against `db.<project>.supabase.co:5432`; connectivity verified via Stage 0 concurrent-DDL probe (CREATE INDEX CONCURRENTLY + DROP INDEX CONCURRENTLY succeeded, see `connectivity-readiness.md`).
- Pre-apply checks (2026-04-20T18:35-18:45Z): baseline.csv captured, presence-before reports 51/51 absent, invalid-before empty, duplicate-before empty, explain-pre captured.
- Query-check gate applied before generating forward SQL; candidates without a committed representative query artifact are deferred.

## Candidate Summary

- Rollout-ready indexes: `51`
- Deferred for missing query check: `0`
- Rollout-ready by schema: `{'admin': 1, 'social': 50}`
- Deferred by schema: `{}`

## Pre-Flight Disk Targets

- `social.instagram_comments` — estimated_row_count: 142782
- `social.tiktok_comments` — estimated_row_count: 75275
- `social.youtube_comments` — estimated_row_count: 30529
- `social.instagram_account_catalog_posts` — estimated_row_count: 29342
- `social.scrape_jobs` — estimated_row_count: 24296

## Rollout Files

- Forward SQL: [`wave-1-forward.sql`](./wave-1-forward.sql)
- Rollback SQL: [`wave-1-rollback.sql`](./wave-1-rollback.sql)

## Baseline Snapshot

- Captured `2026-04-20T18:35:19Z` → `evidence/wave-1/baseline.csv` (4357 rows; wave-1 patterns = `core.shows` + `social.scrape_runs`).
- Post-apply snapshot: `evidence/wave-1/post.csv` (4382 rows).
- Comparison: `evidence/wave-1/compare.csv` (132 queryid deltas; 64 `ok` at ratio 1.0, 68 `insufficient_calls`, 0 regressions).

## Rollout Window

- Started: `2026-04-20T18:47:45Z`
- Finished: `2026-04-20T18:47:56Z`
- Wall time: **11 seconds** (51 CREATE INDEX CONCURRENTLY statements on child tables all under 320 MB; mostly-empty or small-cardinality data fit the fast path).
- Apply session: `application_name = fk-index-wave-1-apply`, backend PID `2020059` (captured by observer snapshot at `evidence/wave-1/loop/20260420T184745Z/`).

## Per-Candidate Apply Outcome

- Indexes requested: 51 (1 admin + 50 social)
- Indexes created successfully: **51 / 51** (apply-log shows one `CREATE INDEX` return per statement)
- Indexes requiring manual re-create: 0
- presence-after verdict: **PASS** (all 51 present + valid + correct columns + correct predicate; 0 flagged rows after predicate-normalization fix landed in commit `59b8706`)
- invalid-after verdict: **PASS** (empty)
- duplicate-after verdict: **PASS** (empty — no new left-prefix duplicates introduced)
- Observer snapshots captured: 1 (at T+00s, build completed before second interval)
- Abort conditions tripped: **None**

## Invalid-Index Cleanup

- Not triggered. No interrupted builds; apply completed `rc=0` in one pass.

## Aborts and Rollbacks

- None.

## Schema-Doc Diffs

- Pending `make schema-docs-check`. Will run after Wave 2 apply so both waves' schema-doc drift (if any) lands in one commit.

## Soak Results

- Soak start: `2026-04-20T18:47:56Z`
- Earliest Wave 2 start (per plan 24h rule): `2026-04-21T18:47:56Z`
- Usage monitoring: operator may query `pg_stat_user_indexes.idx_scan` for the 51 new indexes periodically; low scan counts on FK-protection-only partial indexes are expected and retainable.

## Next Checkpoint

- Either wait for 24h soak before Wave 2 apply (conservative path), or operator may authorize proceeding to Wave 2 immediately given: (a) Wave 1 completed in 11 seconds with no regressions, (b) Wave 2's 38 indexes target different schemas (surveys, firebase_surveys, ml, screenalytics) with independent read paths, (c) all pre-apply checks for Wave 2 are already green.
