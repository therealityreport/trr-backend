begin;

alter table social.twitter_tweets
  add column if not exists bookmarks integer not null default 0,
  add column if not exists shares integer not null default 0,
  add column if not exists thread_root_tweet_id text,
  add column if not exists thread_position integer,
  add column if not exists is_thread_part boolean not null default false,
  add column if not exists twitter_context_role text;

alter table social.twitter_account_catalog_posts
  add column if not exists bookmarks bigint not null default 0,
  add column if not exists thread_root_source_id text,
  add column if not exists thread_position integer,
  add column if not exists is_thread_part boolean not null default false;

comment on column social.twitter_tweets.bookmarks is
  'Public bookmark count only. X/Twitter does not expose private bookmark actors.';

comment on column social.twitter_account_catalog_posts.bookmarks is
  'Public bookmark count only. X/Twitter does not expose private bookmark actors.';

create index if not exists twitter_tweets_thread_root_tweet_id_idx
  on social.twitter_tweets (thread_root_tweet_id)
  where thread_root_tweet_id is not null;

create index if not exists twitter_tweets_twitter_context_role_idx
  on social.twitter_tweets (twitter_context_role)
  where twitter_context_role is not null;

create index if not exists twitter_account_catalog_posts_thread_root_source_id_idx
  on social.twitter_account_catalog_posts (thread_root_source_id)
  where thread_root_source_id is not null;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'twitter_tweets_twitter_context_role_check'
      and conrelid = 'social.twitter_tweets'::regclass
  ) then
    alter table social.twitter_tweets
      add constraint twitter_tweets_twitter_context_role_check
      check (
        twitter_context_role is null
        or twitter_context_role in (
          'account_post',
          'reply_parent',
          'account_reply',
          'audience_reply',
          'quote'
        )
      );
  end if;
end $$;

commit;
