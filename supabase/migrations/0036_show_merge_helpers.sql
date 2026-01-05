begin;

-- =============================================================================
-- Migration 0036: Merge helper for show IDs
-- =============================================================================

create or replace function core.merge_shows(source_show_id uuid, target_show_id uuid)
returns void
language plpgsql
security definer
as $$
begin
  if source_show_id is null or target_show_id is null or source_show_id = target_show_id then
    return;
  end if;

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

  delete from core.shows where id = source_show_id;
end $$;

grant execute on function core.merge_shows(uuid, uuid) to service_role;

commit;
