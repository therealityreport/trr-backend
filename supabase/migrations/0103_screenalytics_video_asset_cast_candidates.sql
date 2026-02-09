-- Migration: Add expected cast candidates per screenalytics video asset

begin;

create schema if not exists screenalytics;

-- Some environments may have schema drift where `screenalytics.video_assets` is
-- missing even though the v2 migration was recorded. Ensure the FK target
-- exists before creating the candidates table.
create table if not exists screenalytics.video_assets (
    id uuid primary key default gen_random_uuid(),
    episode_id uuid null references core.episodes(id) on delete cascade,
    season_id uuid null references core.seasons(id) on delete cascade,
    show_id uuid null references core.shows(id) on delete cascade,
    media_asset_id uuid null references core.media_assets(id) on delete set null,
    source_url text null,
    duration_seconds numeric null,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    check (episode_id is not null or season_id is not null or show_id is not null),
    check (media_asset_id is not null or source_url is not null)
);

create index if not exists screenalytics_video_assets_episode_idx on screenalytics.video_assets (episode_id);
create index if not exists screenalytics_video_assets_season_idx on screenalytics.video_assets (season_id);
create index if not exists screenalytics_video_assets_show_idx on screenalytics.video_assets (show_id);

create table screenalytics.video_asset_cast_candidates (
  video_asset_id uuid not null references screenalytics.video_assets (id) on delete cascade,
  person_id uuid not null references core.people (id) on delete cascade,
  source text not null, -- trr_episode_credits, trr_season_credits, manual, detected
  confidence double precision,
  credit_category text,
  billing_order integer,
  role text,
  added_at timestamptz not null default now(),
  primary key (video_asset_id, person_id)
);

create index idx_sa_vacc_person_id on screenalytics.video_asset_cast_candidates (person_id);

-- Grants (service_role only)
grant usage on schema screenalytics to service_role;
grant all privileges on table screenalytics.video_assets to service_role;
grant all privileges on table screenalytics.video_asset_cast_candidates to service_role;

-- RLS with explicit service_role policies
alter table screenalytics.video_assets enable row level security;
alter table screenalytics.video_asset_cast_candidates enable row level security;

do $$
begin
  if not exists (
    select 1
    from pg_policies
    where schemaname = 'screenalytics'
      and tablename = 'video_assets'
      and policyname = 'service_role_all_video_assets'
  ) then
    create policy "service_role_all_video_assets"
    on screenalytics.video_assets for all to service_role
    using (true) with check (true);
  end if;
end $$;

create policy "service_role_all_video_asset_cast_candidates"
on screenalytics.video_asset_cast_candidates for all to service_role
using (true) with check (true);

commit;
