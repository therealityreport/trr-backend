begin;

create index if not exists ig_posts_season_posted_idx
  on social.instagram_posts (season_id, posted_at desc)
  where season_id is not null;

create index if not exists ig_comments_season_created_idx
  on social.instagram_comments (season_id, created_at desc)
  where season_id is not null;

create index if not exists tt_posts_season_posted_idx
  on social.tiktok_posts (season_id, posted_at desc)
  where season_id is not null;

create index if not exists tt_comments_season_created_idx
  on social.tiktok_comments (season_id, created_at desc)
  where season_id is not null;

create index if not exists yt_videos_season_published_idx
  on social.youtube_videos (season_id, published_at desc)
  where season_id is not null;

create index if not exists yt_comments_season_created_idx
  on social.youtube_comments (season_id, created_at desc)
  where season_id is not null;

create index if not exists tw_tweets_season_reply_created_idx
  on social.twitter_tweets (season_id, is_reply, created_at desc)
  where season_id is not null;

create index if not exists tw_tweets_season_author_norm_idx
  on social.twitter_tweets (season_id, (lower(coalesce(nullif(username, ''), source_account, ''))))
  where season_id is not null;

create index if not exists tw_tweets_season_reply_author_norm_idx
  on social.twitter_tweets (season_id, (lower(coalesce(nullif(source_account, ''), nullif(username, ''), ''))))
  where season_id is not null and is_reply = true;

create index if not exists scrape_jobs_season_created_idx
  on social.scrape_jobs (season_id, created_at desc)
  where season_id is not null;

commit;
