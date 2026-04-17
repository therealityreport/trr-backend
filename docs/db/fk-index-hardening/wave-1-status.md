# Wave-1 FK Index Hardening Status

- generated_at: `2026-04-17T19:10:58Z`
- inventory_source: [`wave-1-inventory.yml`](./wave-1-inventory.yml)
- representative_query_checks: [`scripts/db/fk_index_wave1_explain.sql`](/Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/db/fk_index_wave1_explain.sql)
- connection_mode_used_for_inventory: `runtime`
- resolved_inventory_host: `aws-1-us-east-1.pooler.supabase.com`

## Pre-flight Checks

- Inventory regenerated from the live database on `2026-04-17T19:10:58Z`.
- Inventory ran on `runtime` because direct-host connectivity is currently blocked from this workstation.
- Direct apply / observer lane remains blocked until `db.<project>.supabase.co:5432` is reachable from this machine.
- Query-check gate applied before generating forward SQL; candidates without a committed representative query artifact are deferred.

## Candidate Summary

- Rollout-ready indexes: `51`
- Deferred for missing query check: `0`
- Rollout-ready by schema: `{'admin': 1, 'social': 50}`
- Deferred by schema: `{}`

## Pre-Flight Disk Targets

- `social.instagram_comments` — estimated_row_count: 142843
- `social.tiktok_comments` — estimated_row_count: 75275
- `social.youtube_comments` — estimated_row_count: 30529
- `social.instagram_account_catalog_posts` — estimated_row_count: 29342
- `social.scrape_jobs` — estimated_row_count: 24296

## Rollout Files

- Forward SQL: [`wave-1-forward.sql`](./wave-1-forward.sql)
- Rollback SQL: [`wave-1-rollback.sql`](./wave-1-rollback.sql)

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
