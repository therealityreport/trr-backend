begin;

create table if not exists social.youtube_channel_sync_state (
  id uuid primary key default gen_random_uuid(),
  season_id uuid not null references core.seasons (id) on delete cascade,
  show_id uuid references core.shows (id) on delete cascade,
  source_scope text not null
    check (source_scope in ('bravo', 'creator', 'community')),
  account_handle text not null,
  channel_id text,
  youtube_source_mode text not null default 'hybrid'
    check (youtube_source_mode in ('hybrid', 'api_only', 'scraper_only')),
  last_discovery_source text,
  last_page_token text,
  last_cursor_published_at timestamptz,
  last_discovery_started_at timestamptz,
  last_discovery_completed_at timestamptz,
  last_stats_refresh_at timestamptz,
  last_failure_reason text,
  diagnostics jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (season_id, source_scope, account_handle)
);

create index if not exists idx_youtube_channel_sync_state_season_scope_account
  on social.youtube_channel_sync_state (season_id, source_scope, account_handle);

create index if not exists idx_youtube_channel_sync_state_channel_id
  on social.youtube_channel_sync_state (channel_id)
  where channel_id is not null;

create table if not exists social.youtube_video_sync_state (
  post_id uuid primary key references social.youtube_videos (id) on delete cascade,
  video_id text not null unique,
  season_id uuid references core.seasons (id) on delete cascade,
  show_id uuid references core.shows (id) on delete cascade,
  source_scope text
    check (source_scope is null or source_scope in ('bravo', 'creator', 'community')),
  source_account text,
  last_seen_at timestamptz,
  last_discovery_source text,
  last_stats_checked_at timestamptz,
  last_comments_checked_at timestamptz,
  newest_comment_external_id text,
  newest_comment_published_at timestamptz,
  reply_checkpoint jsonb not null default '{}'::jsonb,
  media_resolution_status text,
  media_resolution_source text,
  media_resolved_at timestamptz,
  media_resolution_attempts jsonb not null default '[]'::jsonb,
  last_mirror_checked_at timestamptz,
  diagnostics jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_youtube_video_sync_state_season_last_seen
  on social.youtube_video_sync_state (season_id, last_seen_at desc)
  where season_id is not null;

create index if not exists idx_youtube_video_sync_state_source_account
  on social.youtube_video_sync_state (source_account, updated_at desc)
  where source_account is not null;

grant select on table social.youtube_channel_sync_state to anon, authenticated;
grant select on table social.youtube_video_sync_state to anon, authenticated;

grant all privileges on table social.youtube_channel_sync_state to service_role;
grant all privileges on table social.youtube_video_sync_state to service_role;

alter table social.youtube_channel_sync_state enable row level security;
alter table social.youtube_video_sync_state enable row level security;

drop policy if exists youtube_channel_sync_state_public_read on social.youtube_channel_sync_state;
create policy youtube_channel_sync_state_public_read on social.youtube_channel_sync_state
for select to anon, authenticated
using (true);

drop policy if exists youtube_video_sync_state_public_read on social.youtube_video_sync_state;
create policy youtube_video_sync_state_public_read on social.youtube_video_sync_state
for select to anon, authenticated
using (true);

commit;
