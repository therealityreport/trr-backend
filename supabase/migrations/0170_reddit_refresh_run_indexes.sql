-- Migration: Add targeted indexes for reddit_refresh_runs hot-path queries
--
-- Context: Several queries in reddit_refresh.py hit social.reddit_refresh_runs
-- without narrow index coverage, causing unnecessary sequential scans:
--
--   1. create_or_reuse_refresh_run() dedup query: selects active runs filtered by
--      (community_id, season_id, period_key, status IN ('queued','running')).
--      The existing composite index includes all statuses, so PG reads all runs
--      for the triple and post-filters.
--
--   2. get_refresh_run() queue position aggregation: counts ALL queued/running
--      runs globally (no community/season filter). The existing status_idx covers
--      this but is wider than needed — a partial index on only active statuses
--      keeps the index small and scan fast.
--
--   3. _fetch_cached_run_row() cache lookup: finds the latest completed/partial
--      run for a (community_id, season_id, period_key) triple. No existing index
--      covers the status filter for completed/partial runs.
--
--   4. Stale-queue recovery UPDATE in create_or_reuse_refresh_run(): filters by
--      (community_id, season_id, period_key, status = 'queued', updated_at < cutoff).
--      Covered by the dedup index below.
--
-- These indexes use IF NOT EXISTS for safety and are created in-transaction so
-- they remain compatible with transaction-scoped reset/apply paths used by CI.

-- 1. Partial composite index for the dedup/reuse check in create_or_reuse_refresh_run().
--    Covers: WHERE community_id = %s AND season_id = %s AND period_key = %s
--            AND status IN ('queued', 'running')
--            ORDER BY created_at DESC
--    Also covers the stale-queue recovery UPDATE (same triple + status = 'queued').
CREATE INDEX IF NOT EXISTS idx_reddit_refresh_runs_dedup
  ON social.reddit_refresh_runs (community_id, season_id, period_key, created_at DESC)
  WHERE status IN ('queued', 'running');

-- 2. Partial index for queue position aggregation in get_refresh_run().
--    Covers: WHERE status IN ('queued', 'running')
--            with count(*) FILTER expressions on status and created_at comparison.
--    Narrow partial index ensures only active rows are scanned.
CREATE INDEX IF NOT EXISTS idx_reddit_refresh_runs_active
  ON social.reddit_refresh_runs (status, created_at)
  WHERE status IN ('queued', 'running');

-- 3. Partial composite index for cache lookups in _fetch_cached_run_row()
--    and _resolve_cached_period_key().
--    Covers: WHERE community_id = %s AND season_id = %s AND period_key = %s
--            AND status IN ('completed', 'partial')
--            ORDER BY created_at DESC / completed_at DESC NULLS LAST
CREATE INDEX IF NOT EXISTS idx_reddit_refresh_runs_cache
  ON social.reddit_refresh_runs (community_id, season_id, period_key, created_at DESC)
  WHERE status IN ('completed', 'partial');
