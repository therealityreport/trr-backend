begin;

-- ---------------------------------------------------------------------------
-- Add source metadata columns to core.shows
-- ---------------------------------------------------------------------------

alter table core.shows add column if not exists tmdb_name text;
alter table core.shows add column if not exists tmdb_status text;
alter table core.shows add column if not exists tmdb_type text;
alter table core.shows add column if not exists tmdb_first_air_date date;
alter table core.shows add column if not exists tmdb_last_air_date date;
alter table core.shows add column if not exists tmdb_vote_average numeric;
alter table core.shows add column if not exists tmdb_vote_count integer;
alter table core.shows add column if not exists tmdb_popularity numeric;

alter table core.shows add column if not exists imdb_title text;
alter table core.shows add column if not exists imdb_content_rating text;
alter table core.shows add column if not exists imdb_rating_value numeric;
alter table core.shows add column if not exists imdb_rating_count integer;
alter table core.shows add column if not exists imdb_date_published date;
alter table core.shows add column if not exists imdb_end_year integer;

alter table core.shows add column if not exists tmdb_fetched_at timestamptz;
alter table core.shows add column if not exists imdb_fetched_at timestamptz;

alter table core.shows add column if not exists tmdb_meta jsonb;
alter table core.shows add column if not exists imdb_meta jsonb;

-- ---------------------------------------------------------------------------
-- TMDb entity ID arrays
-- ---------------------------------------------------------------------------

alter table core.shows add column if not exists tmdb_network_ids integer[];
alter table core.shows add column if not exists tmdb_production_company_ids integer[];

create index if not exists core_shows_tmdb_network_ids_gin
  on core.shows using gin (tmdb_network_ids);

create index if not exists core_shows_tmdb_production_company_ids_gin
  on core.shows using gin (tmdb_production_company_ids);

commit;
