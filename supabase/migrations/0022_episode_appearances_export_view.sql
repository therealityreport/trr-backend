begin;

create schema if not exists core;

drop view if exists core.v_episode_appearances;

create view core.v_episode_appearances as
with base as (
  select distinct
    ea.show_id,
    ea.person_id,
    ea.episode_imdb_id,
    coalesce(e.season_number, ea.season_number) as season_number,
    coalesce(e.episode_number, ea.episode_number) as episode_number,
    e.tmdb_episode_id,
    coalesce(e.air_date, case
      when ea.air_year is not null then make_date(ea.air_year, 1, 1)
      else null
    end) as air_date,
    s.tmdb_season_id
  from core.episode_appearances ea
  left join core.episodes e
    on e.show_id = ea.show_id
    and e.imdb_episode_id = ea.episode_imdb_id
  left join core.seasons s
    on s.id = e.season_id
  where ea.is_archive_footage is false
)
select
  sh.name as show_name,
  p.full_name as cast_member_name,
  coalesce(
    (
      select jsonb_agg(season_number order by season_number)
      from (
        select distinct season_number
        from base b2
        where b2.show_id = base.show_id
          and b2.person_id = base.person_id
          and b2.season_number is not null
        order by season_number
      ) seasons
    ),
    '[]'::jsonb
  ) as seasons,
  coalesce(
    (
      select jsonb_agg(tmdb_season_id order by season_number)
      from (
        select
          season_number,
          min(tmdb_season_id) as tmdb_season_id
        from base b2
        where b2.show_id = base.show_id
          and b2.person_id = base.person_id
          and b2.season_number is not null
          and b2.tmdb_season_id is not null
        group by season_number
        order by season_number
      ) seasons
    ),
    '[]'::jsonb
  ) as tmdb_season_ids,
  sh.tmdb_series_id as tmdb_show_id,
  sh.imdb_series_id as imdb_show_id,
  coalesce(
    (
      select jsonb_agg(episode_imdb_id order by
        season_number nulls last,
        episode_number nulls last,
        air_date nulls last,
        episode_imdb_id
      )
      from (
        select
          episode_imdb_id,
          min(season_number) as season_number,
          min(episode_number) as episode_number,
          min(air_date) as air_date
        from base b2
        where b2.show_id = base.show_id
          and b2.person_id = base.person_id
          and b2.episode_imdb_id is not null
        group by episode_imdb_id
      ) episodes
    ),
    '[]'::jsonb
  ) as imdb_episode_title_ids,
  coalesce(
    (
      select jsonb_agg(tmdb_episode_id order by
        season_number nulls last,
        episode_number nulls last,
        air_date nulls last,
        tmdb_episode_id
      )
      from (
        select
          tmdb_episode_id,
          min(season_number) as season_number,
          min(episode_number) as episode_number,
          min(air_date) as air_date
        from base b2
        where b2.show_id = base.show_id
          and b2.person_id = base.person_id
          and b2.tmdb_episode_id is not null
        group by tmdb_episode_id
      ) episodes
    ),
    '[]'::jsonb
  ) as tmdb_episode_ids
from base
join core.shows sh on sh.id = base.show_id
join core.people p on p.id = base.person_id
group by
  base.show_id,
  base.person_id,
  sh.name,
  p.full_name,
  sh.tmdb_series_id,
  sh.imdb_series_id;

grant select on table core.v_episode_appearances to anon, authenticated, service_role;

commit;
