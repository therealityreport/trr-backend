# Wave-2 FK Index Hardening Status

- generated_at: `2026-04-17T19:22:59Z`
- inventory_source: [`wave-2-inventory.yml`](./wave-2-inventory.yml)
- representative_query_checks: [`scripts/db/fk_index_wave2_explain.sql`](../../scripts/db/fk_index_wave2_explain.sql)
- connection_mode_used_for_inventory: `runtime`
- resolved_inventory_host: `aws-1-us-east-1.pooler.supabase.com`

## Pre-flight Checks

- Inventory regenerated from the live database on `2026-04-17T19:22:59Z`.
- Inventory ran on `runtime` because direct-host connectivity is currently blocked from this workstation.
- Direct apply / observer lane remains blocked until `db.<project>.supabase.co:5432` is reachable from this machine.
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

- Pending. Capture with `scripts/db/run_fk_index_observer.py baseline` once direct connectivity is fixed.

## Per-Candidate Apply Outcome

- Pending direct-lane rollout.

## Invalid-Index Cleanup

- Pending direct-lane rollout.

## Aborts and Rollbacks

- None yet.

## Schema-Doc Diffs

- Pending. Run `make schema-docs-check` after direct apply on the validation target and commit any resulting `supabase/schema_docs/*` drift.

## Soak Results

- Pending. Respect `24h` soak between Wave 1 apply completion and Wave 2 apply start.

## Next Checkpoint

- Do not start apply until direct-connectivity is repaired and baseline snapshots are captured.
