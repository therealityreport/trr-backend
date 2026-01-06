begin;

-- ---------------------------------------------------------------------------
-- Drop legacy per-source show tables (replaced by core.shows)
-- ---------------------------------------------------------------------------

drop table if exists core.imdb_series cascade;
drop table if exists core.tmdb_series cascade;

-- ---------------------------------------------------------------------------
-- Backwards-compatible views (no duplicated storage)
-- ---------------------------------------------------------------------------

alter table core.shows add column if not exists tmdb_meta jsonb;
alter table core.shows add column if not exists imdb_meta jsonb;

create or replace view core.imdb_series as
select
  s.imdb_id as imdb_id,
  s.id as show_id,
  coalesce(s.imdb_title, s.imdb_meta->>'title') as title,
  s.description as description,
  s.imdb_content_rating as content_rating,
  s.imdb_rating_value as rating_value,
  s.imdb_rating_count as rating_count,
  s.imdb_date_published as date_published,
  s.imdb_end_year as end_year,
  s.show_total_seasons as total_seasons,
  s.show_total_episodes as total_episodes,
  case
    when (s.imdb_meta->>'runtime_minutes') ~ '^\d+$' then (s.imdb_meta->>'runtime_minutes')::integer
    else null
  end as runtime_minutes,
  s.imdb_meta->>'trailer_url' as trailer_url,
  s.imdb_meta->>'poster_image_url' as poster_image_url,
  s.imdb_meta->>'poster_image_caption' as poster_image_caption,
  s.imdb_meta->>'imdb_url' as imdb_url,
  s.imdb_fetched_at as fetched_at,
  s.created_at,
  s.updated_at
from core.shows s
where s.imdb_id is not null;

create or replace view core.tmdb_series as
select
  s.tmdb_id as tmdb_id,
  s.id as show_id,
  s.tmdb_name as name,
  s.tmdb_meta->>'original_name' as original_name,
  s.tmdb_meta->>'overview' as overview,
  s.tmdb_meta->>'tagline' as tagline,
  s.tmdb_meta->>'homepage' as homepage,
  s.tmdb_meta->>'original_language' as original_language,
  s.tmdb_popularity as popularity,
  s.tmdb_vote_average as vote_average,
  s.tmdb_vote_count as vote_count,
  s.tmdb_first_air_date as first_air_date,
  s.tmdb_last_air_date as last_air_date,
  s.tmdb_status as status,
  s.tmdb_type as type,
  case
    when lower(s.tmdb_meta->>'in_production') in ('true', 'false')
      then (s.tmdb_meta->>'in_production')::boolean
    else null
  end as in_production,
  case
    when lower(s.tmdb_meta->>'adult') in ('true', 'false')
      then (s.tmdb_meta->>'adult')::boolean
    else null
  end as adult,
  case
    when (s.tmdb_meta->>'number_of_seasons') ~ '^\d+$' then (s.tmdb_meta->>'number_of_seasons')::integer
    else null
  end as number_of_seasons,
  case
    when (s.tmdb_meta->>'number_of_episodes') ~ '^\d+$' then (s.tmdb_meta->>'number_of_episodes')::integer
    else null
  end as number_of_episodes,
  s.tmdb_fetched_at as fetched_at,
  s.created_at,
  s.updated_at
from core.shows s
where s.tmdb_id is not null;

grant select on table core.imdb_series to anon, authenticated;
grant select on table core.tmdb_series to anon, authenticated;
grant all privileges on table core.imdb_series to service_role;
grant all privileges on table core.tmdb_series to service_role;

commit;
