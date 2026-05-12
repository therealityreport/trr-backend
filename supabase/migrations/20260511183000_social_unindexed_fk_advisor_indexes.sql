-- Supabase performance advisor remediation: social unindexed foreign keys.
--
-- Transactional migration form follows this repo's checked-in Supabase
-- migration style for reset/push workflows. These are narrow btree indexes on
-- advisor-confirmed FK columns; no unused indexes are dropped in this pass.
--
-- Rollback note: if any index causes unacceptable write amplification, drop
-- only the affected index in a separate, explicit rollback migration after
-- confirming the advisor finding and query impact. For live production, prefer
-- DROP INDEX CONCURRENTLY IF EXISTS social.<index_name> outside a transaction.

BEGIN;

CREATE INDEX IF NOT EXISTS social_instagram_profile_external_links_last_scrape_job_id_idx
  ON social.instagram_profile_external_links (last_scrape_job_id);

CREATE INDEX IF NOT EXISTS social_instagram_profile_external_links_last_scrape_run_id_idx
  ON social.instagram_profile_external_links (last_scrape_run_id);

CREATE INDEX IF NOT EXISTS social_instagram_profile_relationships_last_scrape_job_id_idx
  ON social.instagram_profile_relationships (last_scrape_job_id);

CREATE INDEX IF NOT EXISTS social_instagram_profile_relationships_last_scrape_run_id_idx
  ON social.instagram_profile_relationships (last_scrape_run_id);

CREATE INDEX IF NOT EXISTS social_instagram_profiles_last_scrape_job_id_idx
  ON social.instagram_profiles (last_scrape_job_id);

CREATE INDEX IF NOT EXISTS social_instagram_profiles_last_scrape_run_id_idx
  ON social.instagram_profiles (last_scrape_run_id);

CREATE INDEX IF NOT EXISTS social_social_post_memberships_assigned_person_id_idx
  ON social.social_post_memberships (assigned_person_id);

CREATE INDEX IF NOT EXISTS social_social_post_memberships_assigned_season_id_idx
  ON social.social_post_memberships (assigned_season_id);

CREATE INDEX IF NOT EXISTS social_social_post_memberships_assigned_show_id_idx
  ON social.social_post_memberships (assigned_show_id);

CREATE INDEX IF NOT EXISTS social_social_post_memberships_last_backfill_run_id_idx
  ON social.social_post_memberships (last_backfill_run_id);

CREATE INDEX IF NOT EXISTS social_social_post_observations_scrape_run_id_idx
  ON social.social_post_observations (scrape_run_id);

COMMIT;
