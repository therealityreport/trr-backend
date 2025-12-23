begin;

create extension if not exists pgcrypto;
create schema if not exists core;

-- Ensure updated_at helper exists (shared across core tables).
create or replace function core.set_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

-- Drop foreign keys that point at shows/seasons/episodes so we can rebuild tables.
do $$
declare
  r record;
begin
  for r in
    select conrelid::regclass as table_name, conname
    from pg_constraint
    where contype = 'f'
      and confrelid in (
        'core.shows'::regclass,
        'core.seasons'::regclass,
        'core.episodes'::regclass
      )
  loop
    execute format('alter table %s drop constraint %I', r.table_name, r.conname);
  end loop;
end $$;

alter table core.episodes rename to episodes_old;
alter table core.seasons rename to seasons_old;
alter table core.shows rename to shows_old;

-- ---------------------------------------------------------------------------
-- core.shows (reordered columns + explicit metadata columns)
-- ---------------------------------------------------------------------------

create table core.shows (
  name text not null,
  network text,
  streaming text,
  show_total_seasons integer,
  show_total_episodes integer,
  imdb_series_id text,
  tmdb_series_id integer,
  most_recent_episode text,
  id uuid primary key default gen_random_uuid(),
  description text,
  premiere_date date,
  external_ids jsonb not null default '{}'::jsonb,
  primary_tmdb_poster_path text,
  primary_tmdb_backdrop_path text,
  primary_tmdb_logo_path text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

insert into core.shows (
  name,
  network,
  streaming,
  show_total_seasons,
  show_total_episodes,
  imdb_series_id,
  tmdb_series_id,
  most_recent_episode,
  id,
  description,
  premiere_date,
  external_ids,
  primary_tmdb_poster_path,
  primary_tmdb_backdrop_path,
  primary_tmdb_logo_path,
  created_at,
  updated_at
)
select
  s.title as name,
  nullif(btrim(s.external_ids->'show_meta'->>'network'), '') as network,
  nullif(btrim(s.external_ids->'show_meta'->>'streaming'), '') as streaming,
  case
    when (s.external_ids->'show_meta'->>'show_total_seasons') ~ '^\d+$'
      then (s.external_ids->'show_meta'->>'show_total_seasons')::int
    else null
  end as show_total_seasons,
  case
    when (s.external_ids->'show_meta'->>'show_total_episodes') ~ '^\d+$'
      then (s.external_ids->'show_meta'->>'show_total_episodes')::int
    else null
  end as show_total_episodes,
  coalesce(
    nullif(btrim(s.external_ids->>'imdb'), ''),
    nullif(btrim(s.external_ids->'show_meta'->>'imdb_series_id'), '')
  ) as imdb_series_id,
  coalesce(
    s.tmdb_id,
    case
      when (s.external_ids->'show_meta'->>'tmdb_series_id') ~ '^\d+$'
        then (s.external_ids->'show_meta'->>'tmdb_series_id')::int
      else null
    end,
    case
      when (s.external_ids->>'tmdb') ~ '^\d+$'
        then (s.external_ids->>'tmdb')::int
      else null
    end
  ) as tmdb_series_id,
  nullif(btrim(s.external_ids->'show_meta'->>'most_recent_episode'), '') as most_recent_episode,
  s.id,
  s.description,
  s.premiere_date,
  s.external_ids,
  s.primary_tmdb_poster_path,
  s.primary_tmdb_backdrop_path,
  s.primary_tmdb_logo_path,
  s.created_at,
  s.updated_at
from core.shows_old s;

drop trigger if exists core_shows_set_updated_at on core.shows;
create trigger core_shows_set_updated_at
before update on core.shows
for each row
execute function core.set_updated_at();

create index if not exists core_shows_external_ids_gin on core.shows using gin (external_ids);

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

do $$
begin
  if exists (
    select 1
    from core.shows
    where imdb_series_id is not null and btrim(imdb_series_id) <> ''
    group by imdb_series_id
    having count(*) > 1
  ) then
    raise exception
      'Duplicate values found in core.shows.imdb_series_id. Resolve duplicates before applying unique index core_shows_imdb_series_id_unique.';
  end if;
end $$;

create unique index if not exists core_shows_imdb_series_id_unique
on core.shows (imdb_series_id)
where imdb_series_id is not null and btrim(imdb_series_id) <> '';

do $$
begin
  if exists (
    select 1
    from core.shows
    where tmdb_series_id is not null
    group by tmdb_series_id
    having count(*) > 1
  ) then
    raise exception
      'Duplicate values found in core.shows.tmdb_series_id. Resolve duplicates before applying unique index core_shows_tmdb_series_id_unique.';
  end if;
end $$;

create unique index if not exists core_shows_tmdb_series_id_unique
on core.shows (tmdb_series_id)
where tmdb_series_id is not null;

-- ---------------------------------------------------------------------------
-- core.seasons (show_name first)
-- ---------------------------------------------------------------------------

-- Avoid index-name collisions when rebuilding core.seasons
do $$
declare
  new_name text;
begin
  if to_regclass('core.seasons_show_id_season_number_unique') is not null then
    new_name := 'seasons_show_id_season_number_unique_old_' || to_char(now(), 'YYYYMMDDHH24MISS');
    execute format('alter index core.seasons_show_id_season_number_unique rename to %I', new_name);
  end if;
  if to_regclass('core.seasons_show_id_idx') is not null then
    new_name := 'seasons_show_id_idx_old_' || to_char(now(), 'YYYYMMDDHH24MISS');
    execute format('alter index core.seasons_show_id_idx rename to %I', new_name);
  end if;
  if to_regclass('core.core_seasons_show_id_season_number_idx') is not null then
    new_name := 'core_seasons_show_id_season_number_idx_old_' || to_char(now(), 'YYYYMMDDHH24MISS');
    execute format('alter index core.core_seasons_show_id_season_number_idx rename to %I', new_name);
  end if;
  if to_regclass('core.core_seasons_tmdb_series_season_unique') is not null then
    new_name := 'core_seasons_tmdb_series_season_unique_old_' || to_char(now(), 'YYYYMMDDHH24MISS');
    execute format('alter index core.core_seasons_tmdb_series_season_unique rename to %I', new_name);
  end if;
end $$;

create table core.seasons (
  show_name text,
  show_id uuid not null references core.shows (id) on delete cascade,
  season_number integer not null check (season_number >= 0),
  name text,
  title text,
  overview text,
  air_date date,
  premiere_date date,
  tmdb_series_id integer,
  imdb_series_id text,
  tmdb_season_id integer,
  tmdb_season_object_id text,
  poster_path text,
  url_original_poster text
    generated always as ('https://image.tmdb.org/t/p/original' || poster_path) stored,
  external_tvdb_id integer,
  external_wikidata_id text,
  external_ids jsonb not null default '{}'::jsonb,
  language text not null default 'en-US',
  fetched_at timestamptz not null default now(),
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint seasons_show_id_season_number_unique unique (show_id, season_number)
);

insert into core.seasons (
  show_name,
  show_id,
  season_number,
  name,
  title,
  overview,
  air_date,
  premiere_date,
  tmdb_series_id,
  imdb_series_id,
  tmdb_season_id,
  tmdb_season_object_id,
  poster_path,
  external_tvdb_id,
  external_wikidata_id,
  external_ids,
  language,
  fetched_at,
  id,
  created_at,
  updated_at
)
select
  coalesce(sh.name, se.show_name) as show_name,
  se.show_id,
  se.season_number,
  se.name,
  se.title,
  se.overview,
  se.air_date,
  se.premiere_date,
  se.tmdb_series_id,
  se.imdb_series_id,
  se.tmdb_season_id,
  se.tmdb_season_object_id,
  se.poster_path,
  se.external_tvdb_id,
  se.external_wikidata_id,
  se.external_ids,
  se.language,
  se.fetched_at,
  se.id,
  se.created_at,
  se.updated_at
from core.seasons_old se
left join core.shows sh on sh.id = se.show_id;

create index if not exists seasons_show_id_idx on core.seasons (show_id);
create index if not exists core_seasons_show_id_season_number_idx on core.seasons (show_id, season_number);

do $$
begin
  if exists (
    select 1
    from core.seasons
    where tmdb_series_id is not null
    group by tmdb_series_id, season_number
    having count(*) > 1
  ) then
    raise exception
      'Duplicate values found in core.seasons (tmdb_series_id, season_number). Resolve duplicates before applying unique index core_seasons_tmdb_series_season_unique.';
  end if;
end $$;

create unique index if not exists core_seasons_tmdb_series_season_unique
on core.seasons (tmdb_series_id, season_number)
where tmdb_series_id is not null;

drop trigger if exists core_seasons_set_updated_at on core.seasons;
create trigger core_seasons_set_updated_at
before update on core.seasons
for each row
execute function core.set_updated_at();

create or replace function core.set_season_show_name()
returns trigger
language plpgsql
as $$
begin
  select s.name into new.show_name
  from core.shows s
  where s.id = new.show_id;
  return new;
end;
$$;

drop trigger if exists core_seasons_set_show_name on core.seasons;
create trigger core_seasons_set_show_name
before insert or update on core.seasons
for each row
execute function core.set_season_show_name();

create or replace function core.propagate_show_name_to_seasons()
returns trigger
language plpgsql
as $$
begin
  update core.seasons
  set show_name = new.name
  where show_id = new.id
    and show_name is distinct from new.name;
  return new;
end;
$$;

drop trigger if exists core_shows_propagate_title_to_seasons on core.shows;
drop trigger if exists core_shows_propagate_name_to_seasons on core.shows;
create trigger core_shows_propagate_name_to_seasons
after update of name on core.shows
for each row
execute function core.propagate_show_name_to_seasons();

-- ---------------------------------------------------------------------------
-- core.episodes (show_name first)
-- ---------------------------------------------------------------------------

-- Avoid index-name collisions when rebuilding core.episodes
do $$
declare
  new_name text;
begin
  if to_regclass('core.episodes_season_id_episode_number_unique') is not null then
    new_name := 'episodes_season_id_episode_number_unique_old_' || to_char(now(), 'YYYYMMDDHH24MISS');
    execute format('alter index core.episodes_season_id_episode_number_unique rename to %I', new_name);
  end if;
  if to_regclass('core.episodes_season_id_idx') is not null then
    new_name := 'episodes_season_id_idx_old_' || to_char(now(), 'YYYYMMDDHH24MISS');
    execute format('alter index core.episodes_season_id_idx rename to %I', new_name);
  end if;
  if to_regclass('core.core_episodes_show_season_idx') is not null then
    new_name := 'core_episodes_show_season_idx_old_' || to_char(now(), 'YYYYMMDDHH24MISS');
    execute format('alter index core.core_episodes_show_season_idx rename to %I', new_name);
  end if;
  if to_regclass('core.core_episodes_show_season_episode_unique') is not null then
    new_name := 'core_episodes_show_season_episode_unique_old_' || to_char(now(), 'YYYYMMDDHH24MISS');
    execute format('alter index core.core_episodes_show_season_episode_unique rename to %I', new_name);
  end if;
  if to_regclass('core.core_episodes_imdb_episode_id_unique') is not null then
    new_name := 'core_episodes_imdb_episode_id_unique_old_' || to_char(now(), 'YYYYMMDDHH24MISS');
    execute format('alter index core.core_episodes_imdb_episode_id_unique rename to %I', new_name);
  end if;
  if to_regclass('core.core_episodes_tmdb_episode_id_unique') is not null then
    new_name := 'core_episodes_tmdb_episode_id_unique_old_' || to_char(now(), 'YYYYMMDDHH24MISS');
    execute format('alter index core.core_episodes_tmdb_episode_id_unique rename to %I', new_name);
  end if;
end $$;

create table core.episodes (
  show_name text,
  show_id uuid not null references core.shows (id) on delete cascade,
  season_number integer not null check (season_number >= 0),
  episode_number integer not null check (episode_number >= 0),
  title text,
  air_date date,
  synopsis text,
  overview text,
  imdb_episode_id text,
  imdb_rating numeric,
  imdb_vote_count integer,
  imdb_primary_image_url text,
  imdb_primary_image_caption text,
  imdb_primary_image_width integer,
  imdb_primary_image_height integer,
  tmdb_series_id integer,
  tmdb_episode_id integer,
  episode_type text,
  production_code text,
  runtime integer,
  still_path text,
  url_original_still text
    generated always as ('https://image.tmdb.org/t/p/original' || still_path) stored,
  tmdb_vote_average numeric,
  tmdb_vote_count integer,
  external_ids jsonb not null default '{}'::jsonb,
  fetched_at timestamptz not null default now(),
  season_id uuid not null references core.seasons (id) on delete cascade,
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint episodes_season_id_episode_number_unique unique (season_id, episode_number)
);

insert into core.episodes (
  show_name,
  show_id,
  season_number,
  episode_number,
  title,
  air_date,
  synopsis,
  overview,
  imdb_episode_id,
  imdb_rating,
  imdb_vote_count,
  imdb_primary_image_url,
  imdb_primary_image_caption,
  imdb_primary_image_width,
  imdb_primary_image_height,
  tmdb_series_id,
  tmdb_episode_id,
  episode_type,
  production_code,
  runtime,
  still_path,
  tmdb_vote_average,
  tmdb_vote_count,
  external_ids,
  fetched_at,
  season_id,
  id,
  created_at,
  updated_at
)
select
  coalesce(sh.name, ep.show_name) as show_name,
  ep.show_id,
  ep.season_number,
  ep.episode_number,
  ep.title,
  ep.air_date,
  ep.synopsis,
  ep.overview,
  ep.imdb_episode_id,
  ep.imdb_rating,
  ep.imdb_vote_count,
  ep.imdb_primary_image_url,
  ep.imdb_primary_image_caption,
  ep.imdb_primary_image_width,
  ep.imdb_primary_image_height,
  ep.tmdb_series_id,
  ep.tmdb_episode_id,
  ep.episode_type,
  ep.production_code,
  ep.runtime,
  ep.still_path,
  ep.tmdb_vote_average,
  ep.tmdb_vote_count,
  ep.external_ids,
  ep.fetched_at,
  ep.season_id,
  ep.id,
  ep.created_at,
  ep.updated_at
from core.episodes_old ep
left join core.shows sh on sh.id = ep.show_id;

create index if not exists episodes_season_id_idx on core.episodes (season_id);
create index if not exists core_episodes_show_season_idx on core.episodes (show_id, season_number, episode_number);

do $$
begin
  if exists (
    select 1
    from core.episodes
    group by show_id, season_number, episode_number
    having count(*) > 1
  ) then
    raise exception
      'Duplicate values found in core.episodes (show_id, season_number, episode_number). Resolve duplicates before applying unique index core_episodes_show_season_episode_unique.';
  end if;
end $$;

create unique index if not exists core_episodes_show_season_episode_unique
on core.episodes (show_id, season_number, episode_number);

do $$
begin
  if exists (
    select 1
    from core.episodes
    where imdb_episode_id is not null
    group by imdb_episode_id
    having count(*) > 1
  ) then
    raise exception
      'Duplicate values found in core.episodes.imdb_episode_id. Resolve duplicates before applying unique index core_episodes_imdb_episode_id_unique.';
  end if;
end $$;

create unique index if not exists core_episodes_imdb_episode_id_unique
on core.episodes (imdb_episode_id)
where imdb_episode_id is not null;

do $$
begin
  if exists (
    select 1
    from core.episodes
    where tmdb_episode_id is not null
    group by tmdb_episode_id
    having count(*) > 1
  ) then
    raise exception
      'Duplicate values found in core.episodes.tmdb_episode_id. Resolve duplicates before applying unique index core_episodes_tmdb_episode_id_unique.';
  end if;
end $$;

create unique index if not exists core_episodes_tmdb_episode_id_unique
on core.episodes (tmdb_episode_id)
where tmdb_episode_id is not null;

drop trigger if exists core_episodes_set_updated_at on core.episodes;
create trigger core_episodes_set_updated_at
before update on core.episodes
for each row
execute function core.set_updated_at();

create or replace function core.set_episode_show_name()
returns trigger
language plpgsql
as $$
begin
  select s.name into new.show_name
  from core.shows s
  where s.id = new.show_id;
  return new;
end;
$$;

drop trigger if exists core_episodes_set_show_name on core.episodes;
create trigger core_episodes_set_show_name
before insert or update on core.episodes
for each row
execute function core.set_episode_show_name();

create or replace function core.propagate_show_name_to_episodes()
returns trigger
language plpgsql
as $$
begin
  update core.episodes
  set show_name = new.name
  where show_id = new.id
    and show_name is distinct from new.name;
  return new;
end;
$$;

drop trigger if exists core_shows_propagate_title_to_episodes on core.shows;
drop trigger if exists core_shows_propagate_name_to_episodes on core.shows;
create trigger core_shows_propagate_name_to_episodes
after update of name on core.shows
for each row
execute function core.propagate_show_name_to_episodes();

-- ---------------------------------------------------------------------------
-- Restore dependent foreign keys (other tables)
-- ---------------------------------------------------------------------------

alter table core.cast_memberships
  add constraint cast_memberships_show_id_fkey
  foreign key (show_id) references core.shows (id) on delete cascade;

alter table core.cast_memberships
  add constraint cast_memberships_season_id_fkey
  foreign key (season_id) references core.seasons (id) on delete cascade;

alter table core.episode_cast
  add constraint episode_cast_episode_id_fkey
  foreign key (episode_id) references core.episodes (id) on delete cascade;

alter table core.show_images
  add constraint show_images_show_id_fkey
  foreign key (show_id) references core.shows (id) on delete cascade;

alter table core.season_images
  add constraint season_images_show_id_fkey
  foreign key (show_id) references core.shows (id) on delete cascade;

alter table core.season_images
  add constraint season_images_season_id_fkey
  foreign key (season_id) references core.seasons (id) on delete cascade;

alter table core.show_cast
  add constraint show_cast_show_id_fkey
  foreign key (show_id) references core.shows (id) on delete cascade;

alter table core.episode_appearances
  add constraint episode_appearances_show_id_fkey
  foreign key (show_id) references core.shows (id) on delete cascade;

alter table games.games
  add constraint games_show_id_fkey
  foreign key (show_id) references core.shows (id) on delete cascade;

alter table games.games
  add constraint games_season_id_fkey
  foreign key (season_id) references core.seasons (id) on delete set null;

alter table games.games
  add constraint games_episode_id_fkey
  foreign key (episode_id) references core.episodes (id) on delete set null;

alter table social.threads
  add constraint threads_episode_id_fkey
  foreign key (episode_id) references core.episodes (id) on delete cascade;

-- ---------------------------------------------------------------------------
-- Grants + RLS policies
-- ---------------------------------------------------------------------------

grant usage on schema core to anon, authenticated, service_role;

grant select on table
  core.shows,
  core.seasons,
  core.episodes
to anon, authenticated;

grant all privileges on table
  core.shows,
  core.seasons,
  core.episodes
to service_role;

alter table core.shows enable row level security;
alter table core.seasons enable row level security;
alter table core.episodes enable row level security;

drop policy if exists core_shows_public_read on core.shows;
create policy core_shows_public_read on core.shows
for select to anon, authenticated
using (true);

drop policy if exists core_seasons_public_read on core.seasons;
create policy core_seasons_public_read on core.seasons
for select to anon, authenticated
using (true);

drop policy if exists core_episodes_public_read on core.episodes;
create policy core_episodes_public_read on core.episodes
for select to anon, authenticated
using (true);

-- ---------------------------------------------------------------------------
-- Update dependent views to use show name column
-- ---------------------------------------------------------------------------

create or replace view core.v_show_seasons as
select
  sh.name as show_name,
  se.*
from core.seasons se
join core.shows sh
  on se.show_id = sh.id;

create or replace view core.v_show_images as
select
  si.id,
  si.show_id,
  si.tmdb_id,
  s.name as show_name,
  si.source,
  si.kind,
  si.iso_639_1,
  si.file_path,
  si.url_original,
  si.width,
  si.height,
  si.aspect_ratio,
  si.fetched_at
from core.show_images si
join core.shows s
  on si.tmdb_id = s.tmdb_series_id;

grant select on table core.v_show_seasons to anon, authenticated, service_role;
grant select on table core.v_show_images to anon, authenticated, service_role;

drop table core.episodes_old;
drop table core.seasons_old;
drop table core.shows_old;

commit;
