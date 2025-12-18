begin;

-- Add a canonical TMDb series id column to `core.shows`.
-- This is used for stable joins (e.g. images) without relying on JSONB keys.

create schema if not exists core;

alter table core.shows add column if not exists tmdb_id integer;

-- Backfill from existing external_ids->>'tmdb' when possible.
update core.shows
set tmdb_id = (external_ids->>'tmdb')::int
where tmdb_id is null
  and coalesce(external_ids->>'tmdb', '') ~ '^[0-9]+$';

-- Guardrail: prevent duplicates before applying unique index.
do $$
begin
  if exists (
    select 1
    from core.shows
    where tmdb_id is not null
    group by tmdb_id
    having count(*) > 1
  ) then
    raise exception
      'Duplicate values found in core.shows.tmdb_id. Resolve duplicates before applying unique index core_shows_tmdb_id_unique.';
  end if;
end $$;

create unique index if not exists core_shows_tmdb_id_unique
on core.shows (tmdb_id)
where tmdb_id is not null;

commit;

