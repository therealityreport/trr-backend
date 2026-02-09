-- Migration 0110: Enrich core.credit_occurrences with per-episode credit metadata
--
-- These fields are sourced from IMDb episodic credits and replace legacy per-episode fields
-- that previously lived on core.episode_appearances (pre-aggregation).

begin;

alter table core.credit_occurrences
  add column if not exists air_year integer,
  add column if not exists credit_text text,
  add column if not exists attributes jsonb not null default '[]'::jsonb,
  add column if not exists is_archive_footage boolean not null default false;

commit;

