# FK Index Hardening Rollout — Closeout

## Handoff Snapshot
```yaml
handoff:
  include: true
  state: recent
  last_updated: 2026-04-20
  current_phase: "closeout complete"
  next_action: "Monitor planner usage for the new indexes over the next 24-48h and merge the rollout branch via PR."
  detail: self
```

Completed: 2026-04-20
Branch: `feat/fk-index-hardening-rollout` (26 commits off `f2529eb`)

## Outcome

All 89 FK-supporting indexes for TRR-owned schemas are now in production:
- Wave 1: 51 indexes (1 `admin` + 50 `social`), applied `2026-04-20T18:47:45Z → 18:47:56Z` (11s)
- Wave 2: 38 indexes (2 `surveys` + 25 `ml` + 9 `screenalytics` + 2 `firebase_surveys`), applied `2026-04-20T18:56:32Z → 18:56:38Z` (6s)

Zero aborts. Zero regressions (ratio threshold = 25% over 30m, no triggering deltas in either wave's `compare.csv`). Zero manual re-creates. Zero left-prefix-duplicate indexes introduced. Zero INVALID state post-apply.

## Post-rollout inventory verification

```
$ PYTHONPATH=. ./.venv/bin/python -m scripts.db.run_fk_index_inventory --wave wave-1 --output /tmp/final-check-wave-1.yml
$ grep -c "decision: add" /tmp/final-check-wave-1.yml
0
$ PYTHONPATH=. ./.venv/bin/python -m scripts.db.run_fk_index_inventory --wave wave-2 --output /tmp/final-check-wave-2.yml
$ grep -c "decision: add" /tmp/final-check-wave-2.yml
0
```

Owned-schema FK debt cleared across all 9 owned schemas (`core`, `admin`, `social`, `surveys`, `firebase_surveys`, `public`, `screenalytics`, `ml`, `pipeline`). `core` and `public` contributed zero candidates at freeze time (already covered).

## Evidence committed

Per-wave under `docs/db/fk-index-hardening/evidence/wave-{1,2}/`:
- `baseline.csv` + `post.csv` + `compare.csv` — `pg_stat_statements` regression track
- `presence-{before,after}.csv` + `invalid-{before,after}.csv` + `duplicate-{before,after}.csv` — presence/validity/prefix-duplicate gates
- `explain-{pre,post}.txt` — representative-query plan captures
- `apply-log.txt` — per-statement `CREATE INDEX CONCURRENTLY` confirmations + UTC timestamps
- `loop/<ts>/*.csv` (Wave 1 only — Wave 2 apply completed before first poll interval)

## Migration history

Pre-staged canonical migrations authored during Stage 6 Task 6.3:
- `supabase/migrations/20260420180200_fk_index_hardening_wave_1.sql` (51 `CREATE INDEX IF NOT EXISTS` in `BEGIN/COMMIT`)
- `supabase/migrations/20260420180300_fk_index_hardening_wave_2.sql` (38 ditto)

Both registered in `supabase_migrations.schema_migrations` via targeted `INSERT` (not `supabase db push --include-all`, which would have tried to apply 4 unrelated pending local migrations from April 9–17 that are out of scope for this rollout). Both now show local | remote | time populated in `supabase migration list`.

## Defects surfaced during rollout (now fixed)

Three SQL defects only manifested when running checks against live production data; they would have blocked every future rollout:

1. **`name[] vs text[]` operator mismatch** in `fk_index_presence_check.sql` + `fk_index_duplicate_check.sql` (`pg_attribute.attname` aggregation had no equality operator against inventory JSON text arrays). Fix: cast `attname::text` in `array_agg`. Commit `859cfe0`.

2. **`%I` / psycopg2 format collision** in `fk_index_invalid_check.sql` (raised "argument formats can't be mixed"). Fix: escape to `%%I` so psycopg2 passes a literal `%I` through to Postgres's `format()`. Commit `859cfe0`.

3. **Partial-predicate text-canonicalization mismatch** in `fk_index_presence_check.sql` (Postgres stores `(col IS NOT NULL)` but inventory records `col is not null`; raw-string comparison yielded 17 false positives). Fix: strip outer parens + collapse whitespace + lowercase before comparing on both sides. Commit `59b8706`.

## Pre-existing drift noted, NOT resolved

`supabase migration list` shows:
- 4 local-only migrations (2026-04-09, -10, -17×2) that have not been applied to production
- 4 remote-only migrations (2026-04-10, -12×3) that exist in production but not in repo

This drift pre-dates the FK hardening branch and is out of scope. It should be addressed as a separate task; see `docs/runbooks/supabase_migration_history_repair.md` for the playbook.

## Pre-existing test failures noted, NOT resolved

`tests/scripts/test_cookie_refresh_worker.py` has 2 date-dependent failures (fixture date vs. 7-day max_cookie_age threshold). Also out of scope; does not affect FK hardening work.

## Observer CLI + per-table labels

The branch adds per-table observer patterns for two multi-table schemas:
- `screenalytics_*` — 7 per-table labels (`face_bank_images`, `identity_locks`, `media_upload_sessions`, `suggestion_applies`, `suggestion_batches`, `unknown_clusters`, `video_assets`). Commit `f6eee71`.
- `ml_*` — 9 per-table labels (`analysis_media_assets`, `analysis_media_cast_candidates`, `analysis_media_upload_sessions`, `face_reference_images`, `screentime_person_metrics`, `screentime_reference_fingerprints`, `screentime_review_state`, `screentime_segments`, `screentime_unknown_clusters`). Commit `dae84fb`.

Replaced broad `%from screenalytics.%` / `%from ml.screentime_runs r%` patterns. Validated in Wave 2 `compare.csv` which shows real matches for `screenalytics_face_bank_images`, `screenalytics_media_upload_sessions`, `screenalytics_video_assets` rows.

## Operator contract changes landed in this branch

- `PGAPPNAME=fk-index-wave-<N>-apply` required before `psql` — **NOTE:** Supavisor rewrites env var to `Supavisor`, so inside-session `SET application_name = 'fk-index-wave-<N>-apply';` is the correct pattern (env var is passed through to direct connections only). Documented in `scripts/db/README.md`.
- Observer loop retries transient DB errors with 3-consecutive-failure abort. Commit `2c9d36e`.
- Evidence CSVs committed despite `*.csv` gitignore (exception added in `.gitignore`: `!docs/db/fk-index-hardening/evidence/**/*.csv`). Commit `e4261a5`.

## Repo validation gates

- `ruff check` + `ruff format --check` on all FK-hardening-scope files: clean.
- `pytest -q tests/scripts/test_fk_index_{wave_artifacts,checks_wrapper,observer_loop_retry}.py`: 20 passed.
- `make schema-docs-check`: no drift (`supabase/schema_docs/` only covers `core` schema which had zero candidates).

## Next steps (post-merge)

- Rebase/squash the 3 fixup commits (`d80e582`, `61421a9`, `0495fc7`) into their predecessors for a cleaner history if the team prefers (optional; branch is readable as-is).
- Merge `feat/fk-index-hardening-rollout` into `main` via PR.
- Monitor `pg_stat_user_indexes.idx_scan` for the 89 new indexes over the next 24–48h to confirm planner usage; indexes with low `idx_scan` are retained as FK-protection infrastructure per plan contract.
