begin;

-- =============================================================================
-- Migration 0029: Create source-specific metadata tables
-- =============================================================================
-- Stores IMDb and TMDb specific data in normalized tables instead of JSONB.

-- ---------------------------------------------------------------------------
-- core.imdb_series - IMDb-specific show metadata
-- ---------------------------------------------------------------------------
create table if not exists core.imdb_series (
  imdb_id text primary key,
  show_id uuid not null references core.shows(id) on delete cascade,
  title text,
  description text,
  content_rating text,
  rating_value numeric,
  rating_count integer,
  date_published date,
  end_year integer,
  total_seasons integer,
  total_episodes integer,
  runtime_minutes integer,
  trailer_url text,
  poster_image_url text,
  poster_image_caption text,
  imdb_url text,
  fetched_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists imdb_series_show_id_idx on core.imdb_series (show_id);

-- Updated_at trigger
drop trigger if exists core_imdb_series_set_updated_at on core.imdb_series;
create trigger core_imdb_series_set_updated_at
  before update on core.imdb_series
  for each row execute function core.set_updated_at();

-- ---------------------------------------------------------------------------
-- core.tmdb_series - TMDb-specific show metadata
-- ---------------------------------------------------------------------------
create table if not exists core.tmdb_series (
  tmdb_id integer primary key,
  show_id uuid not null references core.shows(id) on delete cascade,
  name text,
  original_name text,
  overview text,
  tagline text,
  homepage text,
  original_language text,
  popularity numeric,
  vote_average numeric,
  vote_count integer,
  first_air_date date,
  last_air_date date,
  status text,
  type text,
  in_production boolean,
  adult boolean,
  number_of_seasons integer,
  number_of_episodes integer,
  fetched_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists tmdb_series_show_id_idx on core.tmdb_series (show_id);

-- Updated_at trigger
drop trigger if exists core_tmdb_series_set_updated_at on core.tmdb_series;
create trigger core_tmdb_series_set_updated_at
  before update on core.tmdb_series
  for each row execute function core.set_updated_at();

-- ---------------------------------------------------------------------------
-- core.tmdb_series_external_ids - TMDb external ID mappings (typed, not JSONB)
-- ---------------------------------------------------------------------------
create table if not exists core.tmdb_series_external_ids (
  tmdb_id integer primary key references core.tmdb_series(tmdb_id) on delete cascade,
  imdb_id text,
  tvdb_id integer,
  tvrage_id integer,
  wikidata_id text,
  facebook_id text,
  instagram_id text,
  twitter_id text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

-- Updated_at trigger
drop trigger if exists core_tmdb_series_external_ids_set_updated_at on core.tmdb_series_external_ids;
create trigger core_tmdb_series_external_ids_set_updated_at
  before update on core.tmdb_series_external_ids
  for each row execute function core.set_updated_at();

-- ---------------------------------------------------------------------------
-- Grants and RLS
-- ---------------------------------------------------------------------------
grant select on table core.imdb_series to anon, authenticated;
grant select on table core.tmdb_series to anon, authenticated;
grant select on table core.tmdb_series_external_ids to anon, authenticated;

grant all privileges on table core.imdb_series to service_role;
grant all privileges on table core.tmdb_series to service_role;
grant all privileges on table core.tmdb_series_external_ids to service_role;

alter table core.imdb_series enable row level security;
alter table core.tmdb_series enable row level security;
alter table core.tmdb_series_external_ids enable row level security;

drop policy if exists core_imdb_series_public_read on core.imdb_series;
create policy core_imdb_series_public_read on core.imdb_series
  for select to anon, authenticated using (true);

drop policy if exists core_tmdb_series_public_read on core.tmdb_series;
create policy core_tmdb_series_public_read on core.tmdb_series
  for select to anon, authenticated using (true);

drop policy if exists core_tmdb_series_external_ids_public_read on core.tmdb_series_external_ids;
create policy core_tmdb_series_external_ids_public_read on core.tmdb_series_external_ids
  for select to anon, authenticated using (true);

commit;
