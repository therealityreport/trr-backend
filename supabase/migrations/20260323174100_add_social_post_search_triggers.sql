begin;

create or replace function social.refresh_platform_post_search_fields()
returns trigger
language plpgsql
as $$
declare
  post_json jsonb := to_jsonb(new);
  platform text;
  computed_search_text text;
  computed_search_hashtags text[];
  computed_search_handles text[];
begin
  platform := case tg_table_name
    when 'instagram_posts' then 'instagram'
    when 'tiktok_posts' then 'tiktok'
    when 'youtube_videos' then 'youtube'
    when 'twitter_tweets' then 'twitter'
    when 'facebook_posts' then 'facebook'
    when 'meta_threads_posts' then 'threads'
    else null
  end;

  if platform is null then
    return new;
  end if;

  computed_search_text := social._build_post_search_text(platform, post_json);
  computed_search_hashtags := social._build_post_search_hashtags(platform, post_json, computed_search_text);
  computed_search_handles := social._build_post_search_handles(post_json, computed_search_text);

  new.search_text := coalesce(computed_search_text, '');
  new.search_hashtags := coalesce(computed_search_hashtags, array[]::text[]);
  new.search_handles := coalesce(computed_search_handles, array[]::text[]);
  new.search_handle_identities := coalesce(
    social._build_post_search_handle_identities(
      post_json,
      computed_search_text,
      new.search_hashtags,
      new.search_handles
    ),
    array[]::text[]
  );
  return new;
end;
$$;

drop trigger if exists trg_refresh_instagram_post_search_fields on social.instagram_posts;
create trigger trg_refresh_instagram_post_search_fields
before insert or update on social.instagram_posts
for each row execute function social.refresh_platform_post_search_fields();

drop trigger if exists trg_refresh_tiktok_post_search_fields on social.tiktok_posts;
create trigger trg_refresh_tiktok_post_search_fields
before insert or update on social.tiktok_posts
for each row execute function social.refresh_platform_post_search_fields();

drop trigger if exists trg_refresh_youtube_post_search_fields on social.youtube_videos;
create trigger trg_refresh_youtube_post_search_fields
before insert or update on social.youtube_videos
for each row execute function social.refresh_platform_post_search_fields();

drop trigger if exists trg_refresh_twitter_post_search_fields on social.twitter_tweets;
create trigger trg_refresh_twitter_post_search_fields
before insert or update on social.twitter_tweets
for each row execute function social.refresh_platform_post_search_fields();

drop trigger if exists trg_refresh_facebook_post_search_fields on social.facebook_posts;
create trigger trg_refresh_facebook_post_search_fields
before insert or update on social.facebook_posts
for each row execute function social.refresh_platform_post_search_fields();

drop trigger if exists trg_refresh_threads_post_search_fields on social.meta_threads_posts;
create trigger trg_refresh_threads_post_search_fields
before insert or update on social.meta_threads_posts
for each row execute function social.refresh_platform_post_search_fields();

commit;
