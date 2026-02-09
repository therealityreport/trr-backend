-- Migration 0108: Consolidate core.shows most_recent_episode fields into jsonb and drop redundant columns
--
-- This migration is written to be tolerant of schema drift: it uses IF EXISTS checks
-- around legacy columns that may have been dropped in earlier migrations.

begin;

alter table core.shows
  add column if not exists most_recent_episode_data jsonb not null default '{}'::jsonb;

-- Backfill jsonb from legacy typed columns when present.
do $$
begin
  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'core'
      and table_name = 'shows'
      and column_name in (
        'most_recent_episode_season',
        'most_recent_episode_number',
        'most_recent_episode_title',
        'most_recent_episode_air_date',
        'most_recent_episode_imdb_id'
      )
  ) then
    execute $sql$
      update core.shows
      set most_recent_episode_data = case
        when most_recent_episode_season is null
          and most_recent_episode_number is null
          and most_recent_episode_title is null
          and most_recent_episode_air_date is null
          and most_recent_episode_imdb_id is null
        then '{}'::jsonb
        else jsonb_build_object(
          'imdb',
          jsonb_strip_nulls(
            jsonb_build_object(
              'season', most_recent_episode_season,
              'episode', most_recent_episode_number,
              'title', most_recent_episode_title,
              'air_date', case
                when most_recent_episode_air_date is not null then to_char(most_recent_episode_air_date, 'YYYY-MM-DD')
                else null
              end,
              'imdb_id', most_recent_episode_imdb_id
            )
          )
        )
      end
      where most_recent_episode_data = '{}'::jsonb
        and (
          most_recent_episode_season is not null
          or most_recent_episode_number is not null
          or most_recent_episode_title is not null
          or most_recent_episode_air_date is not null
          or most_recent_episode_imdb_id is not null
        );
    $sql$;
  end if;
end $$;

-- Drop redundant/consolidated columns (if present).
alter table core.shows drop column if exists network;
alter table core.shows drop column if exists streaming;

alter table core.shows drop column if exists most_recent_episode;
alter table core.shows drop column if exists most_recent_episode_season;
alter table core.shows drop column if exists most_recent_episode_number;
alter table core.shows drop column if exists most_recent_episode_title;
alter table core.shows drop column if exists most_recent_episode_air_date;
alter table core.shows drop column if exists most_recent_episode_imdb_id;

alter table core.shows drop column if exists facebook_id;
alter table core.shows drop column if exists instagram_id;
alter table core.shows drop column if exists twitter_id;

alter table core.shows drop column if exists needs_tmdb_resolution;
alter table core.shows drop column if exists needs_imdb_resolution;

-- Rename new column to clean name (only if most_recent_episode doesn't already exist).
do $$
begin
  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'core'
      and table_name = 'shows'
      and column_name = 'most_recent_episode_data'
  ) and not exists (
    select 1
    from information_schema.columns
    where table_schema = 'core'
      and table_name = 'shows'
      and column_name = 'most_recent_episode'
  ) then
    execute 'alter table core.shows rename column most_recent_episode_data to most_recent_episode';
  end if;
end $$;

-- Update merge_shows again to reflect the modified core.shows schema.
create or replace function core.merge_shows(source_show_id uuid, target_show_id uuid)
returns void
language plpgsql
security definer
as $$
declare
  source_row record;
  target_row record;
  rec record;
begin
  if source_show_id is null or target_show_id is null or source_show_id = target_show_id then
    return;
  end if;

  select * into source_row from core.shows where id = source_show_id;
  select * into target_row from core.shows where id = target_show_id;

  if source_row is null then
    return;
  end if;

  update core.shows
  set
    genres = (
      select array_agg(distinct val order by val)
      from unnest(coalesce(target_row.genres, '{}') || coalesce(source_row.genres, '{}')) as val
      where val is not null and val <> ''
    ),
    keywords = (
      select array_agg(distinct val order by val)
      from unnest(coalesce(target_row.keywords, '{}') || coalesce(source_row.keywords, '{}')) as val
      where val is not null and val <> ''
    ),
    tags = (
      select array_agg(distinct val order by val)
      from unnest(coalesce(target_row.tags, '{}') || coalesce(source_row.tags, '{}')) as val
      where val is not null and val <> ''
    ),
    networks = (
      select array_agg(distinct val order by val)
      from unnest(coalesce(target_row.networks, '{}') || coalesce(source_row.networks, '{}')) as val
      where val is not null and val <> ''
    ),
    streaming_providers = (
      select array_agg(distinct val order by val)
      from unnest(coalesce(target_row.streaming_providers, '{}') || coalesce(source_row.streaming_providers, '{}')) as val
      where val is not null and val <> ''
    ),
    listed_on = (
      select array_agg(distinct val order by val)
      from unnest(coalesce(target_row.listed_on, '{}') || coalesce(source_row.listed_on, '{}')) as val
      where val is not null and val <> ''
    ),
    -- Scalar external IDs: prefer target, fallback to source
    tvdb_id = coalesce(target_row.tvdb_id, source_row.tvdb_id),
    tvrage_id = coalesce(target_row.tvrage_id, source_row.tvrage_id),
    wikidata_id = coalesce(target_row.wikidata_id, source_row.wikidata_id),
    -- Also merge primary IDs if target is missing them
    imdb_id = coalesce(target_row.imdb_id, source_row.imdb_id),
    tmdb_id = coalesce(target_row.tmdb_id, source_row.tmdb_id),
    -- Merge most_recent_episode jsonb keyed by source (prefer target keys when overlapping)
    most_recent_episode = coalesce(source_row.most_recent_episode, '{}'::jsonb) || coalesce(target_row.most_recent_episode, '{}'::jsonb)
  where id = target_show_id;

  if to_regclass('core.show_images') is not null then
    delete from core.show_images s
    using core.show_images t
    where s.show_id = source_show_id
      and t.show_id = target_show_id
      and s.source = t.source
      and s.source_image_id = t.source_image_id;

    update core.show_images
    set show_id = target_show_id
    where show_id = source_show_id;
  end if;

  if to_regclass('core.show_cast_overrides') is not null then
    delete from core.show_cast_overrides s
    using core.show_cast_overrides t
    where s.show_id = source_show_id
      and t.show_id = target_show_id
      and s.person_id = t.person_id
      and s.credit_category = t.credit_category;

    update core.show_cast_overrides
    set show_id = target_show_id
    where show_id = source_show_id;
  end if;

  if to_regclass('core.credits') is not null then
    for rec in
      select
        s.id as source_credit_id,
        t.id as target_credit_id
      from core.credits s
      join core.credits t
        on t.show_id = target_show_id
       and s.show_id = source_show_id
       and t.person_id = s.person_id
       and t.credit_category = s.credit_category
       and coalesce(t.role, '') = coalesce(s.role, '')
       and t.source_type = s.source_type
    loop
      delete from core.credit_occurrences co_s
      using core.credit_occurrences co_t
      where co_s.credit_id = rec.source_credit_id
        and co_t.credit_id = rec.target_credit_id
        and co_s.episode_id = co_t.episode_id;

      update core.credit_occurrences
      set credit_id = rec.target_credit_id
      where credit_id = rec.source_credit_id;

      delete from core.credits where id = rec.source_credit_id;
    end loop;

    update core.credits
    set show_id = target_show_id
    where show_id = source_show_id;
  end if;

  if to_regclass('core.season_images') is not null then
    update core.season_images set show_id = target_show_id where show_id = source_show_id;
  end if;

  if to_regclass('core.seasons') is not null then
    delete from core.seasons s
    using core.seasons t
    where s.show_id = source_show_id
      and t.show_id = target_show_id
      and s.season_number = t.season_number;

    update core.seasons set show_id = target_show_id where show_id = source_show_id;
  end if;

  if to_regclass('core.episodes') is not null then
    delete from core.episodes s
    using core.episodes t
    where s.show_id = source_show_id
      and t.show_id = target_show_id
      and s.season_number = t.season_number
      and s.episode_number = t.episode_number;

    delete from core.episodes s
    using core.episodes t
    where s.show_id = source_show_id
      and t.show_id = target_show_id
      and s.imdb_episode_id is not null
      and s.imdb_episode_id = t.imdb_episode_id;

    delete from core.episodes s
    using core.episodes t
    where s.show_id = source_show_id
      and t.show_id = target_show_id
      and s.tmdb_episode_id is not null
      and s.tmdb_episode_id = t.tmdb_episode_id;

    update core.episodes set show_id = target_show_id where show_id = source_show_id;
  end if;

  if to_regclass('core.imdb_series') is not null then
    update core.imdb_series set show_id = target_show_id where show_id = source_show_id;
  end if;

  if to_regclass('core.tmdb_series') is not null then
    update core.tmdb_series set show_id = target_show_id where show_id = source_show_id;
  end if;

  if to_regclass('core.sync_state') is not null then
    update core.sync_state set show_id = target_show_id where show_id = source_show_id;
  end if;

  delete from core.shows where id = source_show_id;
end $$;

grant execute on function core.merge_shows(uuid, uuid) to service_role;

commit;

