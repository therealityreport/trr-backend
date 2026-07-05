-- Local FK-index hardening for SocialBlade Instagram following snapshots.
--
-- Evidence source: checked-in migrations create nullable FK columns on
-- social.instagram_profile_following_snapshots and
-- social.instagram_profile_relationship_snapshot_items after the earlier
-- advisor FK-index waves. This migration only adds narrow btree indexes for
-- those FK columns; it does not drop or rewrite existing indexes.
--
-- Rollback note: if an index causes unacceptable write amplification, drop
-- only that index in a separate rollback migration after confirming live
-- workload impact. For live production, prefer DROP INDEX CONCURRENTLY IF
-- EXISTS social.<index_name> outside a transaction.

BEGIN;

CREATE INDEX IF NOT EXISTS instagram_following_snapshots_last_job_id_idx
  ON social.instagram_profile_following_snapshots (last_scrape_job_id);

CREATE INDEX IF NOT EXISTS instagram_following_snapshots_last_run_id_idx
  ON social.instagram_profile_following_snapshots (last_scrape_run_id);

CREATE INDEX IF NOT EXISTS instagram_relationship_snapshot_items_rel_row_id_idx
  ON social.instagram_profile_relationship_snapshot_items (relationship_row_id);

CREATE INDEX IF NOT EXISTS instagram_relationship_snapshot_items_last_job_id_idx
  ON social.instagram_profile_relationship_snapshot_items (last_scrape_job_id);

CREATE INDEX IF NOT EXISTS instagram_relationship_snapshot_items_last_run_id_idx
  ON social.instagram_profile_relationship_snapshot_items (last_scrape_run_id);

COMMIT;
