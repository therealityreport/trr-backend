-- Migration 0109: Enrich core.people with multi-source canonical fields
--
-- Each field stores values keyed by source (e.g. {"tmdb": "...", "fandom": "..."}).

begin;

alter table core.people
  add column if not exists birthday jsonb not null default '{}'::jsonb,
  add column if not exists gender jsonb not null default '{}'::jsonb,
  add column if not exists biography jsonb not null default '{}'::jsonb,
  add column if not exists place_of_birth jsonb not null default '{}'::jsonb,
  add column if not exists homepage jsonb not null default '{}'::jsonb,
  add column if not exists profile_image_url jsonb not null default '{}'::jsonb;

commit;

