# Wave-2 FK Index Hardening Status

- generated_at: `2026-04-20T18:35:19Z`
- inventory_source: [`wave-2-inventory.yml`](./wave-2-inventory.yml)
- representative_query_checks: [`scripts/db/fk_index_wave2_explain.sql`](../../scripts/db/fk_index_wave2_explain.sql)
- connection_mode_used_for_inventory: `direct`
- resolved_inventory_host: `db.vwxfvzutyufrkhfgoeaa.supabase.co`

## Pre-flight Checks

- Inventory regenerated from the live database on `2026-04-20T18:35:19Z`.
- Inventory ran on `direct` against `db.<project>.supabase.co:5432`; Stage 0 connectivity verified per `connectivity-readiness.md`.
- Pre-apply checks (2026-04-20T18:40Z): baseline.csv captured, presence-before reports 38/38 absent, invalid-before empty, duplicate-before empty, explain-pre captured.
- Query-check gate applied before generating forward SQL; candidates without a committed representative query artifact are deferred.

## Candidate Summary

- Rollout-ready indexes: `38`
- Deferred for missing query check: `0`
- Rollout-ready by schema: `{'firebase_surveys': 2, 'ml': 25, 'screenalytics': 9, 'surveys': 2}`
- Deferred by schema: `{}`

## Pre-Flight Disk Targets

- `screenalytics.media_upload_sessions` — estimated_row_count: 29
- `ml.analysis_media_assets` — estimated_row_count: 28
- `screenalytics.video_assets` — estimated_row_count: 26
- `ml.analysis_media_upload_sessions` — estimated_row_count: 2

## Rollout Files

- Forward SQL: [`wave-2-forward.sql`](./wave-2-forward.sql)
- Rollback SQL: [`wave-2-rollback.sql`](./wave-2-rollback.sql)

## Baseline Snapshot

- Captured `2026-04-20T18:40Z` → `evidence/wave-2/baseline.csv` (82 rows; wave-2 patterns = surveys/pipeline/firebase_surveys + 7 screenalytics per-table + 9 ml per-table).
- Post-apply snapshot: `evidence/wave-2/post.csv` (82 rows).
- Comparison: `evidence/wave-2/compare.csv` (15 queryid deltas; 5 `ok` at ratio 1.0, 10 `insufficient_calls`, 0 regressions).

## Rollout Window

- Wave 1 soak: operator authorized proceeding to Wave 2 immediately (no 24h gap). Rationale: Wave 1 completed 11s with zero regressions; Wave 2 targets independent schemas (surveys, firebase_surveys, ml, screenalytics); Wave 2 pre-apply checks were already green from Stage 3.
- Started: `2026-04-20T18:56:32Z`
- Finished: `2026-04-20T18:56:38Z`
- Wall time: **6 seconds** (38 CREATE INDEX CONCURRENTLY on tables all <30 rows currently; fastest-path concurrent builds).
- Apply session: `application_name = fk-index-wave-2-apply`.

## Per-Candidate Apply Outcome

- Indexes requested: 38 (2 surveys + 25 ml + 9 screenalytics + 2 firebase_surveys)
- Indexes created successfully: **38 / 38**
- Indexes requiring manual re-create: 0
- presence-after verdict: **PASS** (0 flagged rows; predicate-normalization fix from `59b8706` prevents false positives)
- invalid-after verdict: **PASS** (empty)
- duplicate-after verdict: **PASS** (empty — no new left-prefix duplicates)
- Observer snapshots: apply completed before first poll interval; verified via post-apply presence/invalid/duplicate instead.
- Abort conditions tripped: **None**

## Invalid-Index Cleanup

- Not triggered. No interrupted builds.

## Aborts and Rollbacks

- None.

## Schema-Doc Diffs

- Stage 6 will run `make schema-docs-check` after both waves completed; resulting `supabase/schema_docs/*` drift will commit with the closeout handoff.

## Soak Results

- Soak start: `2026-04-20T18:56:38Z`
- Wave-2 soak optional since rollout is complete; `pg_stat_user_indexes.idx_scan` monitoring available via ad-hoc `run_sql.sh -c` queries during normal operation.

## Final FK Coverage Verification

- Wave 1 + Wave 2 total: 89 FK-supporting indexes created (51 + 38).
- Owned-schema `add` candidates remaining: to be re-verified at Stage 6 Task 6.1 by re-running `run_fk_index_inventory` for both waves and counting `decision: add`.

## Next Checkpoint

- Stage 6 closeout: `make schema-docs-check` + register pre-staged migrations via `supabase db push` + `../scripts/handoff-lifecycle.sh closeout`.
