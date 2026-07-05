-- Supabase performance advisor 2026-07-02 reported
-- social.hashtag_assignments.season_id as the only unindexed foreign key.
-- The existing (show_id, season_id) index does not cover lookups by season_id alone.
-- This table is small assignment metadata, and Supabase migration APIs run DDL
-- inside a transaction, so this migration intentionally avoids CONCURRENTLY.
CREATE INDEX IF NOT EXISTS hashtag_assignments_season_id_idx
  ON social.hashtag_assignments USING btree (season_id)
  WHERE season_id IS NOT NULL;
