begin;

create extension if not exists pg_trgm with schema extensions;

create index if not exists instagram_posts_search_text_trgm_idx
  on social.instagram_posts using gin (search_text extensions.gin_trgm_ops);
create index if not exists instagram_posts_search_hashtags_idx
  on social.instagram_posts using gin (search_hashtags);
create index if not exists instagram_posts_search_handles_idx
  on social.instagram_posts using gin (search_handles);
create index if not exists instagram_posts_search_handle_identities_idx
  on social.instagram_posts using gin (search_handle_identities);

create index if not exists tiktok_posts_search_text_trgm_idx
  on social.tiktok_posts using gin (search_text extensions.gin_trgm_ops);
create index if not exists tiktok_posts_search_hashtags_idx
  on social.tiktok_posts using gin (search_hashtags);
create index if not exists tiktok_posts_search_handles_idx
  on social.tiktok_posts using gin (search_handles);
create index if not exists tiktok_posts_search_handle_identities_idx
  on social.tiktok_posts using gin (search_handle_identities);

create index if not exists youtube_videos_search_text_trgm_idx
  on social.youtube_videos using gin (search_text extensions.gin_trgm_ops);
create index if not exists youtube_videos_search_hashtags_idx
  on social.youtube_videos using gin (search_hashtags);
create index if not exists youtube_videos_search_handles_idx
  on social.youtube_videos using gin (search_handles);
create index if not exists youtube_videos_search_handle_identities_idx
  on social.youtube_videos using gin (search_handle_identities);

create index if not exists twitter_tweets_search_text_trgm_idx
  on social.twitter_tweets using gin (search_text extensions.gin_trgm_ops);
create index if not exists twitter_tweets_search_hashtags_idx
  on social.twitter_tweets using gin (search_hashtags);
create index if not exists twitter_tweets_search_handles_idx
  on social.twitter_tweets using gin (search_handles);
create index if not exists twitter_tweets_search_handle_identities_idx
  on social.twitter_tweets using gin (search_handle_identities);

create index if not exists facebook_posts_search_text_trgm_idx
  on social.facebook_posts using gin (search_text extensions.gin_trgm_ops);
create index if not exists facebook_posts_search_hashtags_idx
  on social.facebook_posts using gin (search_hashtags);
create index if not exists facebook_posts_search_handles_idx
  on social.facebook_posts using gin (search_handles);
create index if not exists facebook_posts_search_handle_identities_idx
  on social.facebook_posts using gin (search_handle_identities);

create index if not exists meta_threads_posts_search_text_trgm_idx
  on social.meta_threads_posts using gin (search_text extensions.gin_trgm_ops);
create index if not exists meta_threads_posts_search_hashtags_idx
  on social.meta_threads_posts using gin (search_hashtags);
create index if not exists meta_threads_posts_search_handles_idx
  on social.meta_threads_posts using gin (search_handles);
create index if not exists meta_threads_posts_search_handle_identities_idx
  on social.meta_threads_posts using gin (search_handle_identities);

commit;
