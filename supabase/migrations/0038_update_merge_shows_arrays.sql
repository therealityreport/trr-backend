begin;

-- =============================================================================
-- Migration 0038: Update merge_shows to handle array columns
-- =============================================================================
-- Updates the merge function to:
-- 1. Merge array columns (union unique values, sorted)
-- 2. Prefer target's scalar external IDs, fallback to source
-- 3. AND resolution flags (both must need resolution after merge)
--
-- The child table cascade logic is kept for backward compatibility until
-- migration 0039 drops those tables.

create or replace function core.merge_shows(source_show_id uuid, target_show_id uuid)
returns void
language plpgsql
security definer
as $$
declare
  source_row record;
  target_row record;
begin
  if source_show_id is null or target_show_id is null or source_show_id = target_show_id then
    return;
  end if;

  -- Fetch both rows for array/scalar merging
  select * into source_row from core.shows where id = source_show_id;
  select * into target_row from core.shows where id = target_show_id;

  if source_row is null then
    return;
  end if;

  -- Merge array columns and external IDs into target
  update core.shows
  set
    -- Array columns: union unique values, sorted
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
    facebook_id = coalesce(target_row.facebook_id, source_row.facebook_id),
    instagram_id = coalesce(target_row.instagram_id, source_row.instagram_id),
    twitter_id = coalesce(target_row.twitter_id, source_row.twitter_id),
    -- Also merge primary IDs if target is missing them
    imdb_id = coalesce(target_row.imdb_id, source_row.imdb_id),
    tmdb_id = coalesce(target_row.tmdb_id, source_row.tmdb_id),
    -- Resolution flags: AND logic (only need resolution if both needed it)
    needs_imdb_resolution = coalesce(target_row.needs_imdb_resolution, false) and coalesce(source_row.needs_imdb_resolution, false),
    needs_tmdb_resolution = coalesce(target_row.needs_tmdb_resolution, false) and coalesce(source_row.needs_tmdb_resolution, false)
  where id = target_show_id;

  -- ---------------------------------------------------------------------------
  -- Existing child table cascade logic (kept until 0039 drops these tables)
  -- ---------------------------------------------------------------------------

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

  -- Child attribute tables (will be removed after 0039)
  if to_regclass('core.show_genres') is not null then
    delete from core.show_genres s
    using core.show_genres t
    where s.show_id = source_show_id
      and t.show_id = target_show_id
      and s.source = t.source
      and s.genre = t.genre;

    update core.show_genres set show_id = target_show_id where show_id = source_show_id;
  end if;

  if to_regclass('core.show_keywords') is not null then
    delete from core.show_keywords s
    using core.show_keywords t
    where s.show_id = source_show_id
      and t.show_id = target_show_id
      and s.source = t.source
      and s.keyword = t.keyword;

    update core.show_keywords set show_id = target_show_id where show_id = source_show_id;
  end if;

  if to_regclass('core.show_tags') is not null then
    delete from core.show_tags s
    using core.show_tags t
    where s.show_id = source_show_id
      and t.show_id = target_show_id
      and s.source = t.source
      and s.tag = t.tag;

    update core.show_tags set show_id = target_show_id where show_id = source_show_id;
  end if;

  if to_regclass('core.show_networks') is not null then
    delete from core.show_networks s
    using core.show_networks t
    where s.show_id = source_show_id
      and t.show_id = target_show_id
      and s.network = t.network;

    update core.show_networks set show_id = target_show_id where show_id = source_show_id;
  end if;

  if to_regclass('core.show_streaming_providers') is not null then
    delete from core.show_streaming_providers s
    using core.show_streaming_providers t
    where s.show_id = source_show_id
      and t.show_id = target_show_id
      and s.provider = t.provider
      and s.region = t.region;

    update core.show_streaming_providers
    set show_id = target_show_id
    where show_id = source_show_id;
  end if;

  -- Existing entity cascades
  if to_regclass('core.show_cast') is not null then
    delete from core.show_cast s
    using core.show_cast t
    where s.show_id = source_show_id
      and t.show_id = target_show_id
      and s.person_id = t.person_id
      and s.credit_category = t.credit_category;

    update core.show_cast set show_id = target_show_id where show_id = source_show_id;
  end if;

  if to_regclass('core.cast_memberships') is not null then
    delete from core.cast_memberships s
    using core.cast_memberships t
    where s.show_id = source_show_id
      and t.show_id = target_show_id
      and s.person_id = t.person_id
      and s.role = t.role
      and ((s.season_id is null and t.season_id is null) or s.season_id = t.season_id);

    update core.cast_memberships set show_id = target_show_id where show_id = source_show_id;
  end if;

  if to_regclass('core.episode_appearances') is not null then
    update core.episode_appearances set show_id = target_show_id where show_id = source_show_id;
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

  -- Delete source row
  delete from core.shows where id = source_show_id;
end $$;

grant execute on function core.merge_shows(uuid, uuid) to service_role;

commit;
