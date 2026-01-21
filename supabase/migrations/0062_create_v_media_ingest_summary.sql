-- ---------------------------------------------------------------------------
-- Migration 0062: Create monitoring view for media ingest status
-- ---------------------------------------------------------------------------
-- Provides operational visibility into media asset mirroring progress.
-- ---------------------------------------------------------------------------

begin;

create or replace view core.v_media_ingest_summary as
select
  source,
  ingest_status,
  count(*) as asset_count,
  coalesce(sum(bytes), 0) as total_source_bytes,
  coalesce(sum(hosted_bytes), 0) as total_hosted_bytes,
  round(avg(ingest_retry_count)::numeric, 2) as avg_retries,
  max(ingest_retry_count) as max_retries,
  max(ingest_failed_at) as last_failure_at,
  max(ingest_completed_at) as last_completion_at,
  min(created_at) as oldest_asset_at,
  max(created_at) as newest_asset_at
from core.media_assets
group by source, ingest_status
order by source, ingest_status;

-- Grant access consistent with other core views
grant select on core.v_media_ingest_summary to anon, authenticated, service_role;

commit;
