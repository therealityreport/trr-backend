begin;

-- Ensure core.shows exists with required columns and indexes for show import jobs.
-- Idempotent: safe to apply on existing environments.

create extension if not exists pgcrypto;
create schema if not exists core;

create table if not exists core.shows (
  id uuid primary key default gen_random_uuid(),
  title text not null,
  description text,
  premiere_date date,
  external_ids jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

-- Ensure columns exist (for older deployments).
alter table core.shows add column if not exists description text;
alter table core.shows add column if not exists premiere_date date;
alter table core.shows add column if not exists external_ids jsonb not null default '{}'::jsonb;
alter table core.shows add column if not exists created_at timestamptz not null default now();
alter table core.shows add column if not exists updated_at timestamptz not null default now();

-- Maintain updated_at automatically on updates.
create or replace function core.set_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

drop trigger if exists core_shows_set_updated_at on core.shows;
create trigger core_shows_set_updated_at
before update on core.shows
for each row
execute function core.set_updated_at();

-- Query performance: JSONB GIN index for flexible filtering.
create index if not exists core_shows_external_ids_gin on core.shows using gin (external_ids);

-- Guardrails: prevent duplicate external ids.
do $$
begin
  if exists (
    select 1
    from core.shows
    where coalesce(external_ids->>'imdb', '') <> ''
    group by external_ids->>'imdb'
    having count(*) > 1
  ) then
    raise exception
      'Duplicate values found in core.shows.external_ids->>imdb. Resolve duplicates before applying unique index core_shows_external_ids_imdb_unique.';
  end if;
end $$;

create unique index if not exists core_shows_external_ids_imdb_unique
on core.shows ((external_ids->>'imdb'))
where coalesce(external_ids->>'imdb', '') <> '';

do $$
begin
  if exists (
    select 1
    from core.shows
    where coalesce(external_ids->>'tmdb', '') <> ''
    group by external_ids->>'tmdb'
    having count(*) > 1
  ) then
    raise exception
      'Duplicate values found in core.shows.external_ids->>tmdb. Resolve duplicates before applying unique index core_shows_external_ids_tmdb_unique.';
  end if;
end $$;

create unique index if not exists core_shows_external_ids_tmdb_unique
on core.shows ((external_ids->>'tmdb'))
where coalesce(external_ids->>'tmdb', '') <> '';

commit;

