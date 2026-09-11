begin;

-- These are exactly the Instagram inputs read by _build_post_search_text,
-- _build_post_search_hashtags and _build_post_search_handles. The identity
-- builder consumes their outputs. Metric/raw-observation updates need no work.
drop trigger if exists trg_refresh_instagram_post_search_fields on social.instagram_posts;
create trigger trg_refresh_instagram_post_search_fields
before insert on social.instagram_posts
for each row execute function social.refresh_platform_post_search_fields();

drop trigger if exists trg_refresh_instagram_post_search_fields_update on social.instagram_posts;
create trigger trg_refresh_instagram_post_search_fields_update
before update on social.instagram_posts
for each row when (
  old.caption is distinct from new.caption
  or old.shortcode is distinct from new.shortcode
  or old.hashtags is distinct from new.hashtags
  or old.mentions is distinct from new.mentions
  or old.collaborators is distinct from new.collaborators
  or old.profile_tags is distinct from new.profile_tags
  or old.collaborators_detail is distinct from new.collaborators_detail
)
execute function social.refresh_platform_post_search_fields();

commit;
