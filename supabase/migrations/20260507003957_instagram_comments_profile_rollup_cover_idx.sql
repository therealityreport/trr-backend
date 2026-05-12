create index if not exists instagram_comments_profile_rollup_cover_idx
on social.instagram_comments (post_id)
include (
  is_missing,
  source_snapshot_type,
  last_seen_at,
  scraped_at,
  parent_comment_id,
  parent_comment_external_id,
  reply_depth,
  is_reply
);
