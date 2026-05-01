begin;

-- Phase 2 additive owner-metadata columns for Instagram comments.
--
-- Persisted Apify-source fields that IG already ships in the comment payload
-- but the scraper has not been writing into typed columns. All columns are
-- nullable; older comment rows remain untouched. Column writes in
-- _batch_upsert_instagram_comments and _persist_without_season_context are
-- gated behind _column_exists() so partial deploys remain safe.

alter table social.instagram_comments
  add column if not exists comment_url text,
  add column if not exists author_fbid_v2 text,
  add column if not exists author_is_mentionable boolean,
  add column if not exists author_is_private boolean,
  add column if not exists author_latest_reel_media bigint
    check (author_latest_reel_media is null or author_latest_reel_media >= 0),
  add column if not exists author_profile_pic_id text;

commit;
