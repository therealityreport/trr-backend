-- ---------------------------------------------------------------------------
-- Migration 0061: Add ingest tracking fields to core.media_assets
-- ---------------------------------------------------------------------------
-- Enables async S3 mirroring with status tracking, retry logic, and monitoring.
-- ---------------------------------------------------------------------------

begin;

-- Add ingest tracking columns
alter table core.media_assets
  add column if not exists ingest_status text not null default 'pending',
  add column if not exists ingest_retry_count integer not null default 0,
  add column if not exists ingest_last_error text null,
  add column if not exists ingest_failed_at timestamptz null,
  add column if not exists ingest_completed_at timestamptz null,
  add column if not exists ingest_next_retry_at timestamptz null;

-- CHECK constraint for valid status values
alter table core.media_assets
  add constraint media_assets_ingest_status_valid
  check (ingest_status in ('pending', 'in_progress', 'hosted', 'failed', 'skipped'));

-- Partial index for querying pending/failed assets (the main worker query pattern)
create index if not exists media_assets_ingest_pending_failed_idx
  on core.media_assets (source, ingest_status)
  where ingest_status in ('pending', 'failed');

-- Index for scheduling scans (finding assets ready for retry)
create index if not exists media_assets_ingest_next_retry_idx
  on core.media_assets (ingest_next_retry_at)
  where ingest_status = 'failed' and ingest_next_retry_at is not null;

-- ---------------------------------------------------------------------------
-- Backfill existing data based on current state
-- ---------------------------------------------------------------------------

-- Already hosted: has hosted_url
update core.media_assets
set
  ingest_status = 'hosted',
  ingest_completed_at = coalesce(hosted_at, now())
where hosted_url is not null
  and ingest_status = 'pending';

-- Has source but not hosted: pending (default, no update needed)
-- No source URL: skipped (cannot mirror)
update core.media_assets
set ingest_status = 'skipped'
where source_url is null
  and hosted_url is null
  and ingest_status = 'pending';

commit;
