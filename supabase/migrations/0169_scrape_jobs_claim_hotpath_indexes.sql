-- Migration: Add missing indexes for scrape_jobs claim hot path
--
-- Context: _claim_next_jobs() in social_season_analytics.py runs a CTE
-- every few seconds per worker that scans social.scrape_jobs filtering by
-- status IN ('queued','pending','retrying') AND available_at <= now().
-- Without partial indexes these are sequential scans on every claim cycle.
--
-- These indexes are created CONCURRENTLY and use IF NOT EXISTS for safety.
-- Because CREATE INDEX CONCURRENTLY cannot run inside a transaction block,
-- this migration must be applied outside a transaction (e.g. supabase db push
-- handles this, or run with psql outside BEGIN/COMMIT).

-- 1. Partial index for the eligible-jobs CTE in _claim_next_jobs().
--    Covers: WHERE status IN ('queued','pending','retrying') AND available_at <= now()
--    Ordering: priority ASC, created_at ASC (matches ORDER BY in the candidate CTE).
--    Leading with available_at supports the range predicate (<= now()).
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_scrape_jobs_claim_hotpath
  ON social.scrape_jobs (available_at, priority, created_at)
  WHERE status IN ('queued', 'pending', 'retrying');

-- 2. Partial index for the run_in_flight CTE in _claim_next_jobs().
--    Covers: WHERE status = 'running' AND run_id IS NOT NULL, GROUP BY run_id
--    Leading with run_id supports the GROUP BY; status is in the WHERE partial filter.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_scrape_jobs_running_by_run
  ON social.scrape_jobs (run_id)
  WHERE status = 'running' AND run_id IS NOT NULL;

-- 3. Partial index for stale-heartbeat detection in _recover_stale_running_jobs().
--    Covers: WHERE status = 'running' with ordering by
--    coalesce(heartbeat_at, started_at, claimed_at, created_at).
--    Leading with heartbeat_at helps the COALESCE ordering when heartbeat_at is
--    populated (which is the common case for running jobs that have been claimed).
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_scrape_jobs_heartbeat_stale
  ON social.scrape_jobs (heartbeat_at)
  WHERE status = 'running';
