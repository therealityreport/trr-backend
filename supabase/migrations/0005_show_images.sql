begin;

-- Normalized show images (TMDb posters/logos/backdrops) + primary image columns on core.shows.
-- Idempotent: safe to apply on existing environments.

create extension if not exists pgcrypto;
create schema if not exists core;

-- Convenience columns for a deterministic "primary" image per kind.
alter table core.shows add column if not exists primary_tmdb_poster_path text;
alter table core.shows add column if not exists primary_tmdb_backdrop_path text;
alter table core.shows add column if not exists primary_tmdb_logo_path text;

create table if not exists core.show_images (
  id uuid primary key default gen_random_uuid(),
  show_id uuid not null references core.shows(id) on delete cascade,
  source text not null default 'tmdb',
  kind text not null,
  iso_639_1 text,
  file_path text not null,
  width int,
  height int,
  aspect_ratio numeric,
  vote_average numeric,
  vote_count int,
  fetched_at timestamptz not null default now()
);

-- Enforce allowed image kinds (poster|backdrop|logo).
do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'core_show_images_kind_check'
      and conrelid = 'core.show_images'::regclass
  ) then
    alter table core.show_images
    add constraint core_show_images_kind_check
    check (kind in ('poster', 'backdrop', 'logo'));
  end if;
end $$;

-- Dedupe per show/source/kind/file_path.
create unique index if not exists core_show_images_unique
on core.show_images (show_id, source, kind, file_path);

create index if not exists core_show_images_show_kind_idx
on core.show_images (show_id, kind);

create index if not exists core_show_images_kind_lang_idx
on core.show_images (kind, iso_639_1);

commit;

