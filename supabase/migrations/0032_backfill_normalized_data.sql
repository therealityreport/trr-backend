begin;

-- =============================================================================
-- Migration 0032: Backfill normalized tables from JSONB columns
-- =============================================================================
-- Migrates data from external_ids and imdb_meta JSONB to typed columns/tables.

-- ---------------------------------------------------------------------------
-- 1. Backfill core.shows typed columns from JSONB
-- ---------------------------------------------------------------------------

-- Backfill most_recent_episode columns from show_meta
update core.shows s
set
  most_recent_episode_season = (s.external_ids->'show_meta'->'most_recent_episode_obj'->>'season')::integer,
  most_recent_episode_number = (s.external_ids->'show_meta'->'most_recent_episode_obj'->>'episode')::integer,
  most_recent_episode_title = s.external_ids->'show_meta'->'most_recent_episode_obj'->>'title',
  most_recent_episode_air_date = case
    when s.external_ids->'show_meta'->'most_recent_episode_obj'->>'air_date' ~ '^\d{4}-\d{2}-\d{2}'
    then (s.external_ids->'show_meta'->'most_recent_episode_obj'->>'air_date')::date
    else null
  end,
  most_recent_episode_imdb_id = s.external_ids->'show_meta'->'most_recent_episode_obj'->>'imdb_episode_id'
where s.external_ids->'show_meta'->'most_recent_episode_obj' is not null
  and s.most_recent_episode_season is null;

-- ---------------------------------------------------------------------------
-- 2. Backfill core.imdb_series from imdb_meta column
-- ---------------------------------------------------------------------------

insert into core.imdb_series (
  imdb_id,
  show_id,
  title,
  description,
  content_rating,
  rating_value,
  rating_count,
  date_published,
  total_seasons,
  total_episodes,
  runtime_minutes,
  trailer_url,
  poster_image_url,
  imdb_url,
  fetched_at
)
select
  s.imdb_id,
  s.id as show_id,
  s.imdb_meta->>'title',
  s.imdb_meta->>'description',
  s.imdb_meta->>'content_rating',
  (s.imdb_meta->'aggregate_rating'->>'value')::numeric,
  (s.imdb_meta->'aggregate_rating'->>'count')::integer,
  case
    when s.imdb_meta->>'date_published' ~ '^\d{4}-\d{2}-\d{2}'
    then (s.imdb_meta->>'date_published')::date
    else null
  end,
  (s.imdb_meta->>'total_seasons')::integer,
  (s.imdb_meta->>'total_episodes')::integer,
  (s.imdb_meta->>'runtime_minutes')::integer,
  s.imdb_meta->>'trailer',
  s.imdb_meta->>'poster_image_url',
  s.imdb_meta->>'imdb_url',
  coalesce(
    case when s.imdb_meta->>'fetched_at' ~ '^\d{4}-\d{2}-\d{2}' then (s.imdb_meta->>'fetched_at')::timestamptz else null end,
    now()
  )
from core.shows s
where s.imdb_meta is not null
  and s.imdb_id is not null
  and s.imdb_id <> ''
on conflict (imdb_id) do update set
  title = excluded.title,
  description = excluded.description,
  content_rating = excluded.content_rating,
  rating_value = excluded.rating_value,
  rating_count = excluded.rating_count,
  date_published = excluded.date_published,
  total_seasons = excluded.total_seasons,
  total_episodes = excluded.total_episodes,
  runtime_minutes = excluded.runtime_minutes,
  trailer_url = excluded.trailer_url,
  poster_image_url = excluded.poster_image_url,
  imdb_url = excluded.imdb_url,
  fetched_at = excluded.fetched_at,
  updated_at = now();

-- ---------------------------------------------------------------------------
-- 3. Backfill core.tmdb_series from external_ids->tmdb_meta
-- ---------------------------------------------------------------------------

insert into core.tmdb_series (
  tmdb_id,
  show_id,
  name,
  original_name,
  overview,
  tagline,
  homepage,
  original_language,
  popularity,
  vote_average,
  vote_count,
  first_air_date,
  last_air_date,
  status,
  type,
  in_production,
  adult,
  number_of_seasons,
  number_of_episodes,
  fetched_at
)
select
  s.tmdb_id,
  s.id as show_id,
  s.external_ids->'tmdb_meta'->>'name',
  s.external_ids->'tmdb_meta'->>'original_name',
  s.external_ids->'tmdb_meta'->>'overview',
  s.external_ids->'tmdb_meta'->>'tagline',
  s.external_ids->'tmdb_meta'->>'homepage',
  s.external_ids->'tmdb_meta'->>'original_language',
  (s.external_ids->'tmdb_meta'->>'popularity')::numeric,
  (s.external_ids->'tmdb_meta'->>'vote_average')::numeric,
  (s.external_ids->'tmdb_meta'->>'vote_count')::integer,
  case
    when s.external_ids->'tmdb_meta'->>'first_air_date' ~ '^\d{4}-\d{2}-\d{2}'
    then (s.external_ids->'tmdb_meta'->>'first_air_date')::date
    else null
  end,
  case
    when s.external_ids->'tmdb_meta'->>'last_air_date' ~ '^\d{4}-\d{2}-\d{2}'
    then (s.external_ids->'tmdb_meta'->>'last_air_date')::date
    else null
  end,
  s.external_ids->'tmdb_meta'->>'status',
  s.external_ids->'tmdb_meta'->>'type',
  case
    when s.external_ids->'tmdb_meta'->>'in_production' = 'true' then true
    when s.external_ids->'tmdb_meta'->>'in_production' = 'false' then false
    else null
  end,
  case
    when s.external_ids->'tmdb_meta'->>'adult' = 'true' then true
    when s.external_ids->'tmdb_meta'->>'adult' = 'false' then false
    else null
  end,
  (s.external_ids->'tmdb_meta'->>'number_of_seasons')::integer,
  (s.external_ids->'tmdb_meta'->>'number_of_episodes')::integer,
  now()
from core.shows s
where s.external_ids->'tmdb_meta' is not null
  and s.tmdb_id is not null
on conflict (tmdb_id) do update set
  name = excluded.name,
  original_name = excluded.original_name,
  overview = excluded.overview,
  tagline = excluded.tagline,
  homepage = excluded.homepage,
  original_language = excluded.original_language,
  popularity = excluded.popularity,
  vote_average = excluded.vote_average,
  vote_count = excluded.vote_count,
  first_air_date = excluded.first_air_date,
  last_air_date = excluded.last_air_date,
  status = excluded.status,
  type = excluded.type,
  in_production = excluded.in_production,
  adult = excluded.adult,
  number_of_seasons = excluded.number_of_seasons,
  number_of_episodes = excluded.number_of_episodes,
  fetched_at = excluded.fetched_at,
  updated_at = now();

-- ---------------------------------------------------------------------------
-- 4. Backfill core.tmdb_series_external_ids
-- ---------------------------------------------------------------------------

insert into core.tmdb_series_external_ids (
  tmdb_id,
  imdb_id,
  tvdb_id,
  tvrage_id,
  wikidata_id,
  facebook_id,
  instagram_id,
  twitter_id
)
select
  s.tmdb_id,
  s.external_ids->'tmdb_meta'->'external_ids'->>'imdb_id',
  (s.external_ids->'tmdb_meta'->'external_ids'->>'tvdb_id')::integer,
  (s.external_ids->'tmdb_meta'->'external_ids'->>'tvrage_id')::integer,
  s.external_ids->'tmdb_meta'->'external_ids'->>'wikidata_id',
  s.external_ids->'tmdb_meta'->'external_ids'->>'facebook_id',
  s.external_ids->'tmdb_meta'->'external_ids'->>'instagram_id',
  s.external_ids->'tmdb_meta'->'external_ids'->>'twitter_id'
from core.shows s
where s.tmdb_id is not null
  and s.external_ids->'tmdb_meta'->'external_ids' is not null
  and exists (select 1 from core.tmdb_series ts where ts.tmdb_id = s.tmdb_id)
on conflict (tmdb_id) do update set
  imdb_id = excluded.imdb_id,
  tvdb_id = excluded.tvdb_id,
  tvrage_id = excluded.tvrage_id,
  wikidata_id = excluded.wikidata_id,
  facebook_id = excluded.facebook_id,
  instagram_id = excluded.instagram_id,
  twitter_id = excluded.twitter_id,
  updated_at = now();

-- ---------------------------------------------------------------------------
-- 5. Backfill core.show_genres from imdb_meta->'genres' array
-- ---------------------------------------------------------------------------

insert into core.show_genres (show_id, source, genre)
select s.id, 'imdb', g.genre
from core.shows s,
     lateral jsonb_array_elements_text(s.imdb_meta->'genres') as g(genre)
where s.imdb_meta->'genres' is not null
  and jsonb_typeof(s.imdb_meta->'genres') = 'array'
on conflict (show_id, source, genre) do nothing;

-- Also backfill from TMDb genres if available
insert into core.show_genres (show_id, source, genre)
select s.id, 'tmdb', g.elem->>'name'
from core.shows s,
     lateral jsonb_array_elements(s.external_ids->'tmdb_meta'->'genres') as g(elem)
where s.external_ids->'tmdb_meta'->'genres' is not null
  and jsonb_typeof(s.external_ids->'tmdb_meta'->'genres') = 'array'
  and g.elem->>'name' is not null
on conflict (show_id, source, genre) do nothing;

-- ---------------------------------------------------------------------------
-- 6. Backfill core.show_keywords from imdb_meta->'keywords' array
-- ---------------------------------------------------------------------------

insert into core.show_keywords (show_id, source, keyword)
select s.id, 'imdb', k.keyword
from core.shows s,
     lateral jsonb_array_elements_text(s.imdb_meta->'keywords') as k(keyword)
where s.imdb_meta->'keywords' is not null
  and jsonb_typeof(s.imdb_meta->'keywords') = 'array'
on conflict (show_id, source, keyword) do nothing;

-- ---------------------------------------------------------------------------
-- 7. Backfill core.show_tags from imdb_meta->'tags' array
-- ---------------------------------------------------------------------------

insert into core.show_tags (show_id, source, tag)
select s.id, 'imdb', t.tag
from core.shows s,
     lateral jsonb_array_elements_text(s.imdb_meta->'tags') as t(tag)
where s.imdb_meta->'tags' is not null
  and jsonb_typeof(s.imdb_meta->'tags') = 'array'
on conflict (show_id, source, tag) do nothing;

-- ---------------------------------------------------------------------------
-- 8. Backfill core.show_streaming_providers from shows.streaming column
-- ---------------------------------------------------------------------------

insert into core.show_streaming_providers (show_id, provider, region, provider_type)
select s.id, trim(p.provider), 'US', 'flatrate'
from core.shows s,
     lateral unnest(string_to_array(s.streaming, ',')) as p(provider)
where s.streaming is not null
  and s.streaming <> ''
  and trim(p.provider) <> ''
on conflict (show_id, provider, region) do nothing;

-- ---------------------------------------------------------------------------
-- 9. Backfill core.show_networks from shows.network column
-- ---------------------------------------------------------------------------

insert into core.show_networks (show_id, network, is_primary)
select s.id, s.network, true
from core.shows s
where s.network is not null
  and s.network <> ''
on conflict (show_id, network) do nothing;

-- Also backfill from TMDb networks array if available
insert into core.show_networks (show_id, network, tmdb_network_id, logo_path, origin_country, is_primary)
select
  s.id,
  n.elem->>'name',
  (n.elem->>'id')::integer,
  n.elem->>'logo_path',
  n.elem->>'origin_country',
  false
from core.shows s,
     lateral jsonb_array_elements(s.external_ids->'tmdb_meta'->'networks') as n(elem)
where s.external_ids->'tmdb_meta'->'networks' is not null
  and jsonb_typeof(s.external_ids->'tmdb_meta'->'networks') = 'array'
  and n.elem->>'name' is not null
on conflict (show_id, network) do update set
  tmdb_network_id = excluded.tmdb_network_id,
  logo_path = excluded.logo_path,
  origin_country = excluded.origin_country;

commit;
