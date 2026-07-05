-- Reviewed social unused-index cleanup wave 1.
--
-- Decision matrix:
--   docs/workspace/social-unused-index-decision-matrix-2026-07-02.csv
-- Live production SQL:
--   docs/workspace/social-unused-index-owner-review-2026-07-02/phase3-approved-drops.sql
-- Rollback SQL is captured in the same owner-review packet.
--
-- Production apply must use DROP INDEX CONCURRENTLY outside a transaction.
-- This migration keeps plain DROP INDEX IF EXISTS for fresh database replays.

drop index if exists social.instagram_comments_username_created_idx;
drop index if exists social.idx_social_youtube_comments_season_created_at;
drop index if exists social.yt_comments_season_created_idx;
drop index if exists social.tt_comments_season_created_idx;
drop index if exists social.social_post_entities_lookup_idx;
drop index if exists social.social_post_observations_source_idx;
