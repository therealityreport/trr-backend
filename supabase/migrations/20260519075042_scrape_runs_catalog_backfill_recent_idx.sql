-- Accelerate recent catalog-run lookups used by social account catalog freshness
-- and diagnostics. This keeps the index narrow by covering only active
-- shared-account catalog backfill runs ordered by the UI's recent-run sort.

CREATE INDEX IF NOT EXISTS scrape_runs_catalog_backfill_recent_idx
ON social.scrape_runs (created_at DESC, id DESC)
WHERE coalesce(config->>'pipeline_ingest_mode', '') = 'shared_account_catalog_backfill'
  AND nullif(coalesce(config->>'failure_dismissed_at', ''), '') IS NULL;
