begin;

-- Backend-owned home for the schema DDL previously executed by the TRR-APP
-- shows repository during request handling.
create or replace function public.set_updated_at_timestamp()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

alter table public.survey_shows
  add column if not exists trr_show_id uuid;

alter table public.survey_shows
  add column if not exists fonts jsonb not null default '{}'::jsonb;

create unique index if not exists idx_survey_shows_trr_show_id_unique
  on public.survey_shows (trr_show_id)
  where trr_show_id is not null;

create index if not exists idx_survey_shows_trr_show_id
  on public.survey_shows (trr_show_id);

create table if not exists public.survey_show_palette_library (
  id uuid primary key default gen_random_uuid(),
  trr_show_id uuid not null,
  season_number integer,
  name text not null,
  colors jsonb not null default '[]'::jsonb,
  source_type text not null,
  source_image_url text,
  seed integer not null,
  marker_points jsonb not null default '[]'::jsonb,
  created_by_uid text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint survey_show_palette_library_season_number_valid
    check (season_number is null or season_number > 0),
  constraint survey_show_palette_library_source_type_valid
    check (source_type in ('upload', 'url', 'media_library'))
);

create unique index if not exists idx_survey_show_palette_library_name_scope
  on public.survey_show_palette_library (trr_show_id, coalesce(season_number, -1), lower(name));

create index if not exists idx_survey_show_palette_library_show
  on public.survey_show_palette_library (trr_show_id);

create index if not exists idx_survey_show_palette_library_show_season
  on public.survey_show_palette_library (trr_show_id, season_number);

drop trigger if exists trg_survey_show_palette_library_updated_at
  on public.survey_show_palette_library;

create trigger trg_survey_show_palette_library_updated_at
before update on public.survey_show_palette_library
for each row
execute function public.set_updated_at_timestamp();

commit;
