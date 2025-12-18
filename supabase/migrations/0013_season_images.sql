begin;

-- Season posters (TMDb) stored as one image per row (no credits).

create extension if not exists pgcrypto;
create schema if not exists core;

create table if not exists core.season_images (
  id uuid primary key default gen_random_uuid(),
  show_id uuid not null references core.shows(id) on delete cascade,
  season_id uuid not null references core.seasons(id) on delete cascade,
  tmdb_series_id integer not null,
  season_number integer not null,
  source text not null default 'tmdb',
  kind text not null default 'poster',
  iso_639_1 text,
  file_path text not null,
  url_original text generated always as ('https://image.tmdb.org/t/p/original' || file_path) stored,
  width integer not null,
  height integer not null,
  aspect_ratio numeric not null,
  fetched_at timestamptz not null default now()
);

-- Only posters for now.
do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'core_season_images_kind_check'
      and conrelid = 'core.season_images'::regclass
  ) then
    alter table core.season_images
    add constraint core_season_images_kind_check
    check (kind = 'poster');
  end if;
end $$;

-- Dedupe per (series, season, file_path).
create unique index if not exists core_season_images_unique
on core.season_images (tmdb_series_id, season_number, source, file_path);

create index if not exists core_season_images_season_id_idx
on core.season_images (season_id);

create index if not exists core_season_images_tmdb_series_season_idx
on core.season_images (tmdb_series_id, season_number);

-- Grants + RLS (matches core.show_images pattern).
grant usage on schema core to anon, authenticated, service_role;
grant select on table core.season_images to anon, authenticated;
grant all privileges on table core.season_images to service_role;

alter table core.season_images enable row level security;

drop policy if exists core_season_images_public_read on core.season_images;
create policy core_season_images_public_read on core.season_images
for select to anon, authenticated
using (true);

commit;

