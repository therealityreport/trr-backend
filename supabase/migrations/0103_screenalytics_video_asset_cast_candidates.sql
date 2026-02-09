-- Migration: Add expected cast candidates per screenalytics video asset

begin;

create schema if not exists screenalytics;

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
grant all privileges on table screenalytics.video_asset_cast_candidates to service_role;

-- RLS with explicit service_role policies
alter table screenalytics.video_asset_cast_candidates enable row level security;

create policy "service_role_all_video_asset_cast_candidates"
on screenalytics.video_asset_cast_candidates for all to service_role
using (true) with check (true);

commit;

