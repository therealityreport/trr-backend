begin;

alter table social.tiktok_posts
  add column if not exists sound_id text,
  add column if not exists sound_title text,
  add column if not exists sound_author text,
  add column if not exists sound_usage_count integer not null default 0,
  add column if not exists quality_score numeric,
  add column if not exists quality_flags jsonb not null default '[]'::jsonb,
  add column if not exists velocity_1h numeric,
  add column if not exists velocity_24h numeric,
  add column if not exists velocity_7d numeric,
  add column if not exists cast_member_mentions jsonb not null default '[]'::jsonb;

create index if not exists idx_tiktok_posts_sound_id on social.tiktok_posts (sound_id);
create index if not exists idx_tiktok_posts_season_sound on social.tiktok_posts (season_id, sound_id);

create table if not exists social.tiktok_sounds (
  id uuid primary key default gen_random_uuid(),
  sound_id text not null unique,
  title text,
  artist_name text,
  usage_count integer not null default 0,
  source_url text,
  last_seen_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists social.tiktok_sound_posts (
  id uuid primary key default gen_random_uuid(),
  sound_id text not null references social.tiktok_sounds (sound_id) on delete cascade,
  platform_post_id text not null,
  creator_handle text,
  posted_at timestamptz,
  views integer not null default 0,
  likes integer not null default 0,
  comments integer not null default 0,
  shares integer not null default 0,
  thumbnail_url text,
  caption text,
  raw_data jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (sound_id, platform_post_id)
);

create index if not exists idx_tiktok_sound_posts_sound_id on social.tiktok_sound_posts (sound_id);
create index if not exists idx_tiktok_sound_posts_posted_at on social.tiktok_sound_posts (posted_at desc);

create table if not exists social.tiktok_post_cast_members (
  id uuid primary key default gen_random_uuid(),
  post_id uuid not null references social.tiktok_posts (id) on delete cascade,
  cast_member_id uuid references core.people (id) on delete set null,
  cast_member_name text,
  confidence numeric,
  source text not null default 'auto' check (source in ('auto', 'manual')),
  run_id uuid references social.scrape_runs (id) on delete set null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (post_id, cast_member_id, source)
);

create index if not exists idx_tiktok_post_cast_members_post_id on social.tiktok_post_cast_members (post_id);
create index if not exists idx_tiktok_post_cast_members_cast_member_id on social.tiktok_post_cast_members (cast_member_id);

create table if not exists social.tiktok_post_tokens (
  id uuid primary key default gen_random_uuid(),
  post_id uuid not null references social.tiktok_posts (id) on delete cascade,
  token text not null,
  token_type text not null check (token_type in ('hashtag', 'keyword', 'mention')),
  normalized_token text not null,
  created_at timestamptz not null default now(),
  unique (post_id, token_type, normalized_token)
);

create index if not exists idx_tiktok_post_tokens_post_id on social.tiktok_post_tokens (post_id);
create index if not exists idx_tiktok_post_tokens_type_norm on social.tiktok_post_tokens (token_type, normalized_token);

create table if not exists social.tiktok_post_quality (
  post_id uuid primary key references social.tiktok_posts (id) on delete cascade,
  quality_score numeric,
  missing_fields jsonb not null default '[]'::jsonb,
  confidence_flags jsonb not null default '[]'::jsonb,
  updated_at timestamptz not null default now()
);

create table if not exists social.tiktok_post_velocity (
  post_id uuid primary key references social.tiktok_posts (id) on delete cascade,
  velocity_1h numeric,
  velocity_24h numeric,
  velocity_7d numeric,
  snapshot_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists social.tiktok_post_comment_enrichment (
  post_id uuid primary key references social.tiktok_posts (id) on delete cascade,
  positive_count integer not null default 0,
  neutral_count integer not null default 0,
  negative_count integer not null default 0,
  toxicity_count integer not null default 0,
  cast_mentions_count integer not null default 0,
  updated_at timestamptz not null default now()
);

create table if not exists social.social_ingest_checkpoints (
  id uuid primary key default gen_random_uuid(),
  platform text not null,
  creator_id text not null,
  cursor text,
  last_posted_at timestamptz,
  metadata jsonb not null default '{}'::jsonb,
  updated_at timestamptz not null default now(),
  unique (platform, creator_id)
);

create index if not exists idx_social_ingest_checkpoints_platform_creator
  on social.social_ingest_checkpoints (platform, creator_id);

create table if not exists social.tiktok_anomaly_events (
  id uuid primary key default gen_random_uuid(),
  season_id uuid references core.seasons (id) on delete cascade,
  post_id uuid references social.tiktok_posts (id) on delete cascade,
  account text,
  event_type text not null,
  severity text not null default 'warn',
  event_value numeric,
  baseline_value numeric,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists idx_tiktok_anomaly_events_season_created
  on social.tiktok_anomaly_events (season_id, created_at desc);

create or replace view social.v_tiktok_daily_analytics as
select
  p.season_id,
  p.source_account,
  date_trunc('day', p.posted_at)::date as day,
  count(*)::int as posts,
  coalesce(sum(coalesce(p.views, 0)), 0)::bigint as views,
  coalesce(sum(coalesce(p.likes, 0)), 0)::bigint as likes,
  coalesce(sum(coalesce(p.comments_count, 0)), 0)::bigint as comments,
  coalesce(sum(coalesce(p.shares, 0)), 0)::bigint as shares,
  coalesce(sum(coalesce(p.saves, 0)), 0)::bigint as saves,
  coalesce(avg(nullif(p.quality_score, 0)), 0)::numeric as avg_quality_score
from social.tiktok_posts p
where p.posted_at is not null
group by p.season_id, p.source_account, date_trunc('day', p.posted_at)::date;

create or replace view social.v_tiktok_weekly_analytics as
select
  p.season_id,
  p.source_account,
  date_trunc('week', p.posted_at)::date as week_start,
  count(*)::int as posts,
  coalesce(sum(coalesce(p.views, 0)), 0)::bigint as views,
  coalesce(sum(coalesce(p.likes, 0)), 0)::bigint as likes,
  coalesce(sum(coalesce(p.comments_count, 0)), 0)::bigint as comments,
  coalesce(sum(coalesce(p.shares, 0)), 0)::bigint as shares,
  coalesce(sum(coalesce(p.saves, 0)), 0)::bigint as saves,
  coalesce(avg(nullif(p.quality_score, 0)), 0)::numeric as avg_quality_score
from social.tiktok_posts p
where p.posted_at is not null
group by p.season_id, p.source_account, date_trunc('week', p.posted_at)::date;

grant select on table social.tiktok_sounds to anon, authenticated;
grant select on table social.tiktok_sound_posts to anon, authenticated;
grant select on table social.tiktok_post_cast_members to anon, authenticated;
grant select on table social.tiktok_post_tokens to anon, authenticated;
grant select on table social.tiktok_post_quality to anon, authenticated;
grant select on table social.tiktok_post_velocity to anon, authenticated;
grant select on table social.tiktok_post_comment_enrichment to anon, authenticated;
grant select on table social.social_ingest_checkpoints to anon, authenticated;
grant select on table social.tiktok_anomaly_events to anon, authenticated;
grant select on table social.v_tiktok_daily_analytics to anon, authenticated;
grant select on table social.v_tiktok_weekly_analytics to anon, authenticated;

grant all privileges on table social.tiktok_sounds to service_role;
grant all privileges on table social.tiktok_sound_posts to service_role;
grant all privileges on table social.tiktok_post_cast_members to service_role;
grant all privileges on table social.tiktok_post_tokens to service_role;
grant all privileges on table social.tiktok_post_quality to service_role;
grant all privileges on table social.tiktok_post_velocity to service_role;
grant all privileges on table social.tiktok_post_comment_enrichment to service_role;
grant all privileges on table social.social_ingest_checkpoints to service_role;
grant all privileges on table social.tiktok_anomaly_events to service_role;

alter table social.tiktok_sounds enable row level security;
alter table social.tiktok_sound_posts enable row level security;
alter table social.tiktok_post_cast_members enable row level security;
alter table social.tiktok_post_tokens enable row level security;
alter table social.tiktok_post_quality enable row level security;
alter table social.tiktok_post_velocity enable row level security;
alter table social.tiktok_post_comment_enrichment enable row level security;
alter table social.social_ingest_checkpoints enable row level security;
alter table social.tiktok_anomaly_events enable row level security;

drop policy if exists tiktok_sounds_public_read on social.tiktok_sounds;
create policy tiktok_sounds_public_read on social.tiktok_sounds for select to anon, authenticated using (true);

drop policy if exists tiktok_sound_posts_public_read on social.tiktok_sound_posts;
create policy tiktok_sound_posts_public_read on social.tiktok_sound_posts for select to anon, authenticated using (true);

drop policy if exists tiktok_post_cast_members_public_read on social.tiktok_post_cast_members;
create policy tiktok_post_cast_members_public_read on social.tiktok_post_cast_members for select to anon, authenticated using (true);

drop policy if exists tiktok_post_tokens_public_read on social.tiktok_post_tokens;
create policy tiktok_post_tokens_public_read on social.tiktok_post_tokens for select to anon, authenticated using (true);

drop policy if exists tiktok_post_quality_public_read on social.tiktok_post_quality;
create policy tiktok_post_quality_public_read on social.tiktok_post_quality for select to anon, authenticated using (true);

drop policy if exists tiktok_post_velocity_public_read on social.tiktok_post_velocity;
create policy tiktok_post_velocity_public_read on social.tiktok_post_velocity for select to anon, authenticated using (true);

drop policy if exists tiktok_post_comment_enrichment_public_read on social.tiktok_post_comment_enrichment;
create policy tiktok_post_comment_enrichment_public_read on social.tiktok_post_comment_enrichment for select to anon, authenticated using (true);

drop policy if exists social_ingest_checkpoints_public_read on social.social_ingest_checkpoints;
create policy social_ingest_checkpoints_public_read on social.social_ingest_checkpoints for select to anon, authenticated using (true);

drop policy if exists tiktok_anomaly_events_public_read on social.tiktok_anomaly_events;
create policy tiktok_anomaly_events_public_read on social.tiktok_anomaly_events for select to anon, authenticated using (true);

commit;
