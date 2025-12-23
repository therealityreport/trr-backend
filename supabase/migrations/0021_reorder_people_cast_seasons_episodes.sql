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

-- Drop foreign keys that point at shows/seasons/episodes/people/show_cast/episode_appearances
-- so we can rebuild tables safely.
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
        'core.episodes'::regclass,
        'core.people'::regclass,
        'core.show_cast'::regclass,
        'core.episode_appearances'::regclass
      )
  loop
    execute format('alter table %s drop constraint %I', r.table_name, r.conname);
  end loop;
end $$;

alter table core.episode_appearances rename to episode_appearances_old;
alter table core.show_cast rename to show_cast_old;
alter table core.episodes rename to episodes_old;
alter table core.seasons rename to seasons_old;
alter table core.people rename to people_old;

-- ---------------------------------------------------------------------------
-- core.people (full_name first)
-- ---------------------------------------------------------------------------

create table core.people (
  full_name text not null,
  known_for text,
  external_ids jsonb not null default '{}'::jsonb,
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

insert into core.people (
  full_name,
  known_for,
  external_ids,
  id,
  created_at,
  updated_at
)
select
  p.full_name,
  p.known_for,
  p.external_ids,
  p.id,
  p.created_at,
  p.updated_at
from core.people_old p;

create index if not exists people_full_name_idx on core.people (full_name);

do $$
begin
  if exists (
    select 1
    from core.people
    where external_ids ? 'imdb'
      and btrim(external_ids->>'imdb') <> ''
    group by external_ids->>'imdb'
    having count(*) > 1
  ) then
    raise exception
      'Duplicate values found in core.people external_ids->>imdb. Resolve duplicates before applying unique index core_people_imdb_unique.';
  end if;
end $$;

create unique index if not exists core_people_imdb_unique
on core.people ((external_ids->>'imdb'))
where external_ids ? 'imdb'
  and btrim(external_ids->>'imdb') <> '';

drop trigger if exists core_people_set_updated_at on core.people;
create trigger core_people_set_updated_at
before update on core.people
for each row
execute function core.set_updated_at();

-- ---------------------------------------------------------------------------
-- core.seasons (show_name, name, season_number first)
-- ---------------------------------------------------------------------------

create table core.seasons (
  show_name text,
  name text,
  season_number integer not null check (season_number >= 0),
  show_id uuid not null references core.shows (id) on delete cascade,
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
  name,
  season_number,
  show_id,
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
  se.name,
  se.season_number,
  se.show_id,
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

-- ---------------------------------------------------------------------------
-- core.episodes (show_name, title, season/episode numbers first)
-- ---------------------------------------------------------------------------

create table core.episodes (
  show_name text,
  title text,
  season_number integer not null check (season_number >= 0),
  episode_number integer not null check (episode_number >= 0),
  show_id uuid not null references core.shows (id) on delete cascade,
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
  title,
  season_number,
  episode_number,
  show_id,
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
  ep.title,
  ep.season_number,
  ep.episode_number,
  ep.show_id,
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

-- ---------------------------------------------------------------------------
-- core.show_cast (show_name, cast_member_name first)
-- ---------------------------------------------------------------------------

create table core.show_cast (
  show_name text,
  cast_member_name text,
  show_id uuid not null references core.shows (id) on delete cascade,
  person_id uuid not null references core.people (id) on delete cascade,
  billing_order integer,
  role text,
  credit_category text not null default 'Self',
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (show_id, person_id, credit_category)
);

insert into core.show_cast (
  show_name,
  cast_member_name,
  show_id,
  person_id,
  billing_order,
  role,
  credit_category,
  id,
  created_at,
  updated_at
)
select
  sh.name as show_name,
  p.full_name as cast_member_name,
  sc.show_id,
  sc.person_id,
  sc.billing_order,
  sc.role,
  sc.credit_category,
  sc.id,
  sc.created_at,
  sc.updated_at
from core.show_cast_old sc
left join core.shows sh on sh.id = sc.show_id
left join core.people p on p.id = sc.person_id;

create index if not exists core_show_cast_show_id_idx
on core.show_cast (show_id);

create index if not exists core_show_cast_person_id_idx
on core.show_cast (person_id);

drop trigger if exists core_show_cast_set_updated_at on core.show_cast;
create trigger core_show_cast_set_updated_at
before update on core.show_cast
for each row
execute function core.set_updated_at();

create or replace function core.set_show_cast_names()
returns trigger
language plpgsql
as $$
begin
  select s.name into new.show_name
  from core.shows s
  where s.id = new.show_id;

  select p.full_name into new.cast_member_name
  from core.people p
  where p.id = new.person_id;
  return new;
end;
$$;

drop trigger if exists core_show_cast_set_names on core.show_cast;
create trigger core_show_cast_set_names
before insert or update on core.show_cast
for each row
execute function core.set_show_cast_names();

-- ---------------------------------------------------------------------------
-- core.episode_appearances (show_name, cast_member_name, episode_title first)
-- ---------------------------------------------------------------------------

create table core.episode_appearances (
  show_name text,
  cast_member_name text,
  episode_title text,
  show_id uuid not null references core.shows (id) on delete cascade,
  person_id uuid not null references core.people (id) on delete cascade,
  episode_imdb_id text not null,
  season_number integer,
  episode_number integer,
  air_year integer,
  credit_category text not null default 'Self',
  credit_text text,
  attributes jsonb not null default '[]'::jsonb,
  is_archive_footage boolean not null default false,
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (show_id, person_id, episode_imdb_id, credit_category)
);

insert into core.episode_appearances (
  show_name,
  cast_member_name,
  episode_title,
  show_id,
  person_id,
  episode_imdb_id,
  season_number,
  episode_number,
  air_year,
  credit_category,
  credit_text,
  attributes,
  is_archive_footage,
  id,
  created_at,
  updated_at
)
select
  sh.name as show_name,
  p.full_name as cast_member_name,
  ea.episode_title,
  ea.show_id,
  ea.person_id,
  ea.episode_imdb_id,
  ea.season_number,
  ea.episode_number,
  ea.air_year,
  ea.credit_category,
  ea.credit_text,
  ea.attributes,
  ea.is_archive_footage,
  ea.id,
  ea.created_at,
  ea.updated_at
from core.episode_appearances_old ea
left join core.shows sh on sh.id = ea.show_id
left join core.people p on p.id = ea.person_id;

create index if not exists core_episode_appearances_show_id_idx
on core.episode_appearances (show_id);

create index if not exists core_episode_appearances_person_id_idx
on core.episode_appearances (person_id);

create index if not exists core_episode_appearances_episode_imdb_id_idx
on core.episode_appearances (episode_imdb_id);

drop trigger if exists core_episode_appearances_set_updated_at on core.episode_appearances;
create trigger core_episode_appearances_set_updated_at
before update on core.episode_appearances
for each row
execute function core.set_updated_at();

create or replace function core.set_episode_appearance_names()
returns trigger
language plpgsql
as $$
begin
  select s.name into new.show_name
  from core.shows s
  where s.id = new.show_id;

  select p.full_name into new.cast_member_name
  from core.people p
  where p.id = new.person_id;
  return new;
end;
$$;

drop trigger if exists core_episode_appearances_set_names on core.episode_appearances;
create trigger core_episode_appearances_set_names
before insert or update on core.episode_appearances
for each row
execute function core.set_episode_appearance_names();

-- ---------------------------------------------------------------------------
-- Propagate show/person renames
-- ---------------------------------------------------------------------------

create or replace function core.propagate_show_name_to_dependents()
returns trigger
language plpgsql
as $$
begin
  update core.seasons
  set show_name = new.name
  where show_id = new.id
    and show_name is distinct from new.name;

  update core.episodes
  set show_name = new.name
  where show_id = new.id
    and show_name is distinct from new.name;

  update core.show_cast
  set show_name = new.name
  where show_id = new.id
    and show_name is distinct from new.name;

  update core.episode_appearances
  set show_name = new.name
  where show_id = new.id
    and show_name is distinct from new.name;

  return new;
end;
$$;

drop trigger if exists core_shows_propagate_title_to_seasons on core.shows;
drop trigger if exists core_shows_propagate_name_to_seasons on core.shows;
drop trigger if exists core_shows_propagate_title_to_episodes on core.shows;
drop trigger if exists core_shows_propagate_name_to_episodes on core.shows;
drop trigger if exists core_shows_propagate_name_to_dependents on core.shows;
create trigger core_shows_propagate_name_to_dependents
after update of name on core.shows
for each row
execute function core.propagate_show_name_to_dependents();

create or replace function core.propagate_person_name_to_dependents()
returns trigger
language plpgsql
as $$
begin
  update core.show_cast
  set cast_member_name = new.full_name
  where person_id = new.id
    and cast_member_name is distinct from new.full_name;

  update core.episode_appearances
  set cast_member_name = new.full_name
  where person_id = new.id
    and cast_member_name is distinct from new.full_name;

  return new;
end;
$$;

drop trigger if exists core_people_propagate_name_to_dependents on core.people;
create trigger core_people_propagate_name_to_dependents
after update of full_name on core.people
for each row
execute function core.propagate_person_name_to_dependents();

-- ---------------------------------------------------------------------------
-- Restore dependent foreign keys (other tables)
-- ---------------------------------------------------------------------------

alter table core.cast_memberships
  add constraint cast_memberships_show_id_fkey
  foreign key (show_id) references core.shows (id) on delete cascade;

alter table core.cast_memberships
  add constraint cast_memberships_season_id_fkey
  foreign key (season_id) references core.seasons (id) on delete cascade;

alter table core.cast_memberships
  add constraint cast_memberships_person_id_fkey
  foreign key (person_id) references core.people (id) on delete cascade;

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

alter table games.games
  add constraint games_show_id_fkey
  foreign key (show_id) references core.shows (id) on delete cascade;

alter table games.games
  add constraint games_season_id_fkey
  foreign key (season_id) references core.seasons (id) on delete set null;

alter table games.games
  add constraint games_episode_id_fkey
  foreign key (episode_id) references core.episodes (id) on delete set null;

alter table surveys.surveys
  add constraint surveys_show_id_fkey
  foreign key (show_id) references core.shows (id) on delete cascade;

alter table surveys.surveys
  add constraint surveys_season_id_fkey
  foreign key (season_id) references core.seasons (id) on delete set null;

alter table surveys.surveys
  add constraint surveys_episode_id_fkey
  foreign key (episode_id) references core.episodes (id) on delete set null;

alter table social.threads
  add constraint threads_episode_id_fkey
  foreign key (episode_id) references core.episodes (id) on delete cascade;

-- ---------------------------------------------------------------------------
-- Grants + RLS policies (recreated tables)
-- ---------------------------------------------------------------------------

grant usage on schema core to anon, authenticated, service_role;

grant select on table
  core.people,
  core.seasons,
  core.episodes,
  core.show_cast,
  core.episode_appearances
to anon, authenticated;

grant all privileges on table
  core.people,
  core.seasons,
  core.episodes,
  core.show_cast,
  core.episode_appearances
to service_role;

alter table core.people enable row level security;
alter table core.seasons enable row level security;
alter table core.episodes enable row level security;
alter table core.show_cast enable row level security;
alter table core.episode_appearances enable row level security;

drop policy if exists core_people_public_read on core.people;
create policy core_people_public_read on core.people
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

drop policy if exists core_show_cast_public_read on core.show_cast;
create policy core_show_cast_public_read on core.show_cast
for select to anon, authenticated
using (true);

drop policy if exists core_episode_appearances_public_read on core.episode_appearances;
create policy core_episode_appearances_public_read on core.episode_appearances
for select to anon, authenticated
using (true);

drop table core.episode_appearances_old;
drop table core.show_cast_old;
drop table core.episodes_old;
drop table core.seasons_old;
drop table core.people_old;

commit;
