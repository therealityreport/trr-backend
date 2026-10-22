-- Production: create these indexes CONCURRENTLY before recording this migration.
-- IF NOT EXISTS then makes the migration metadata-only. Do not block ingestion
-- on a non-concurrent build. Rollback leaves these additive indexes in place.
begin;
set local lock_timeout = '5s';
create index if not exists scrape_jobs_profile_catalog_recent_idx
on social.scrape_jobs (
  platform,
  lower(coalesce(nullif(config->>'account', ''), nullif(metadata->>'account', ''), '')),
  (coalesce(completed_at, started_at, created_at)) desc,
  id desc
);
create index if not exists scrape_jobs_profile_comments_account_stage_idx
on social.scrape_jobs (
  platform,
  ltrim(lower(coalesce(config->>'account', metadata->>'account', '')), '@'),
  (coalesce(config->>'stage', metadata->>'stage', job_type)),
  run_id
);
commit;
