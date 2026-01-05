begin;

-- =============================================================================
-- Migration 0037: Collapse show attribute tables into core.shows columns
-- =============================================================================
-- This migration adds array columns to core.shows for genres, keywords, tags,
-- networks, and streaming providers. It also adds external ID columns and
-- list provenance tracking.
--
-- After this migration, data is backfilled from child tables. Once code is
-- updated, migration 0039 will drop the child tables.

-- ---------------------------------------------------------------------------
-- 1. Add array columns for attributes
-- ---------------------------------------------------------------------------

alter table core.shows add column if not exists genres text[];
alter table core.shows add column if not exists keywords text[];
alter table core.shows add column if not exists tags text[];
alter table core.shows add column if not exists networks text[];
alter table core.shows add column if not exists streaming_providers text[];

-- ---------------------------------------------------------------------------
-- 2. Add listed_on for list provenance tracking
-- ---------------------------------------------------------------------------
-- Values: 'imdb', 'tmdb' - indicates which list(s) the show appeared in

alter table core.shows add column if not exists listed_on text[];

-- ---------------------------------------------------------------------------
-- 3. Add external IDs from TMDb (denormalized from tmdb_series_external_ids)
-- ---------------------------------------------------------------------------

alter table core.shows add column if not exists tvdb_id integer;
alter table core.shows add column if not exists tvrage_id integer;
alter table core.shows add column if not exists wikidata_id text;
alter table core.shows add column if not exists facebook_id text;
alter table core.shows add column if not exists instagram_id text;
alter table core.shows add column if not exists twitter_id text;

-- ---------------------------------------------------------------------------
-- 4. Add resolution flags
-- ---------------------------------------------------------------------------
-- needs_imdb_resolution already exists from earlier migration
-- Add needs_tmdb_resolution for parity

alter table core.shows add column if not exists needs_tmdb_resolution boolean not null default false;

-- ---------------------------------------------------------------------------
-- 5. Create GIN indexes for efficient array queries
-- ---------------------------------------------------------------------------

create index if not exists core_shows_genres_gin on core.shows using gin (genres);
create index if not exists core_shows_keywords_gin on core.shows using gin (keywords);
create index if not exists core_shows_tags_gin on core.shows using gin (tags);
create index if not exists core_shows_networks_gin on core.shows using gin (networks);
create index if not exists core_shows_streaming_providers_gin on core.shows using gin (streaming_providers);
create index if not exists core_shows_listed_on_gin on core.shows using gin (listed_on);

-- ---------------------------------------------------------------------------
-- 6. Backfill from child tables (if they exist and have data)
-- ---------------------------------------------------------------------------

-- Backfill genres (merge imdb + tmdb sources, dedupe, sort)
update core.shows s
set genres = coalesce((
  select array_agg(distinct sg.genre order by sg.genre)
  from core.show_genres sg
  where sg.show_id = s.id
), '{}')
where s.genres is null
  and exists (select 1 from core.show_genres where show_id = s.id);

-- Backfill keywords
update core.shows s
set keywords = coalesce((
  select array_agg(distinct sk.keyword order by sk.keyword)
  from core.show_keywords sk
  where sk.show_id = s.id
), '{}')
where s.keywords is null
  and exists (select 1 from core.show_keywords where show_id = s.id);

-- Backfill tags
update core.shows s
set tags = coalesce((
  select array_agg(distinct st.tag order by st.tag)
  from core.show_tags st
  where st.show_id = s.id
), '{}')
where s.tags is null
  and exists (select 1 from core.show_tags where show_id = s.id);

-- Backfill networks
update core.shows s
set networks = coalesce((
  select array_agg(distinct sn.network order by sn.network)
  from core.show_networks sn
  where sn.show_id = s.id
), '{}')
where s.networks is null
  and exists (select 1 from core.show_networks where show_id = s.id);

-- Backfill streaming providers (all regions, flatrate type preferred)
update core.shows s
set streaming_providers = coalesce((
  select array_agg(distinct sp.provider order by sp.provider)
  from core.show_streaming_providers sp
  where sp.show_id = s.id
), '{}')
where s.streaming_providers is null
  and exists (select 1 from core.show_streaming_providers where show_id = s.id);

-- ---------------------------------------------------------------------------
-- 7. Backfill listed_on from ID presence
-- ---------------------------------------------------------------------------
-- If a show has imdb_id, it came from IMDb list; if tmdb_id, from TMDb list

update core.shows s
set listed_on = (
  select array_remove(array[
    case when s.imdb_id is not null and btrim(s.imdb_id) <> '' then 'imdb' else null end,
    case when s.tmdb_id is not null then 'tmdb' else null end
  ], null)
)
where s.listed_on is null;

-- ---------------------------------------------------------------------------
-- 8. Backfill external IDs from tmdb_series_external_ids
-- ---------------------------------------------------------------------------

update core.shows s
set
  tvdb_id = coalesce(s.tvdb_id, ext.tvdb_id),
  tvrage_id = coalesce(s.tvrage_id, ext.tvrage_id),
  wikidata_id = coalesce(s.wikidata_id, ext.wikidata_id),
  facebook_id = coalesce(s.facebook_id, ext.facebook_id),
  instagram_id = coalesce(s.instagram_id, ext.instagram_id),
  twitter_id = coalesce(s.twitter_id, ext.twitter_id)
from core.tmdb_series ts
join core.tmdb_series_external_ids ext on ext.tmdb_id = ts.tmdb_id
where ts.show_id = s.id
  and (
    s.tvdb_id is null or s.tvrage_id is null or s.wikidata_id is null
    or s.facebook_id is null or s.instagram_id is null or s.twitter_id is null
  );

-- ---------------------------------------------------------------------------
-- 9. Set needs_tmdb_resolution for shows with imdb_id but no tmdb_id
-- ---------------------------------------------------------------------------

update core.shows
set needs_tmdb_resolution = true
where imdb_id is not null
  and btrim(imdb_id) <> ''
  and tmdb_id is null
  and needs_tmdb_resolution = false;

commit;
