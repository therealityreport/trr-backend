begin;

create table if not exists social.scrape_workers (
  worker_id text primary key,
  stage text,
  status text not null default 'idle'
    check (status in ('starting', 'idle', 'working', 'stopped')),
  run_id uuid references social.scrape_runs (id) on delete set null,
  current_job_id uuid references social.scrape_jobs (id) on delete set null,
  metadata jsonb not null default '{}'::jsonb,
  started_at timestamptz not null default now(),
  last_seen_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists scrape_workers_status_seen_idx
  on social.scrape_workers (status, last_seen_at desc);

grant select on table social.scrape_workers to anon, authenticated;
grant all privileges on table social.scrape_workers to service_role;

alter table social.scrape_workers enable row level security;

drop policy if exists scrape_workers_public_read on social.scrape_workers;
create policy scrape_workers_public_read on social.scrape_workers
for select to anon, authenticated
using (true);

-- Clean up malformed external IDs before adding hard guardrails.
delete from social.instagram_comments where btrim(comment_id) = '';
delete from social.tiktok_comments where btrim(comment_id) = '';
delete from social.youtube_comments where btrim(comment_id) = '';
delete from social.twitter_tweets where btrim(tweet_id) = '';

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'instagram_comments_comment_id_not_blank'
      and conrelid = 'social.instagram_comments'::regclass
  ) then
    alter table social.instagram_comments
      add constraint instagram_comments_comment_id_not_blank check (btrim(comment_id) <> '');
  end if;

  if not exists (
    select 1
    from pg_constraint
    where conname = 'tiktok_comments_comment_id_not_blank'
      and conrelid = 'social.tiktok_comments'::regclass
  ) then
    alter table social.tiktok_comments
      add constraint tiktok_comments_comment_id_not_blank check (btrim(comment_id) <> '');
  end if;

  if not exists (
    select 1
    from pg_constraint
    where conname = 'youtube_comments_comment_id_not_blank'
      and conrelid = 'social.youtube_comments'::regclass
  ) then
    alter table social.youtube_comments
      add constraint youtube_comments_comment_id_not_blank check (btrim(comment_id) <> '');
  end if;

  if not exists (
    select 1
    from pg_constraint
    where conname = 'twitter_tweets_tweet_id_not_blank'
      and conrelid = 'social.twitter_tweets'::regclass
  ) then
    alter table social.twitter_tweets
      add constraint twitter_tweets_tweet_id_not_blank check (btrim(tweet_id) <> '');
  end if;
end $$;

commit;
