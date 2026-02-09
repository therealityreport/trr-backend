-- Migration 0107: Drop legacy cast tables (show_cast, episode_appearances, cast_memberships, episode_cast)
--
-- Credits model is canonical:
-- - core.credits
-- - core.credit_occurrences
--
-- This migration also:
-- - Updates core.merge_shows to merge credits/occurrences (and stop touching legacy cast tables)
-- - Updates name-propagation triggers to stop updating legacy cast tables
-- - Recreates core.v_episode_appearances as an alias over credits-based validation view

begin;

-- ---------------------------------------------------------------------------
-- 1) Stop name-propagation triggers from touching dropped tables
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

  return new;
end;
$$;

create or replace function core.propagate_person_name_to_dependents()
returns trigger
language plpgsql
as $$
begin
  -- Legacy denormalized tables (show_cast, episode_appearances) are removed.
  -- Keep trigger for backwards compatibility but make it a no-op.
  return new;
end;
$$;

-- ---------------------------------------------------------------------------
-- 2) Update merge_shows to operate on credits model
-- ---------------------------------------------------------------------------

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

  -- Fetch both rows for array/scalar merging
  select * into source_row from core.shows where id = source_show_id;
  select * into target_row from core.shows where id = target_show_id;

  if source_row is null then
    return;
  end if;

  -- Merge array columns and scalar IDs into target
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
  -- Cascade to remaining tables
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

  -- Show-cast overrides are keyed by (show_id, person_id, credit_category)
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

  -- Canonical credits model: merge credits (and re-point occurrences) before moving remaining credits
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
      -- Avoid primary-key conflicts when moving occurrences.
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

  -- Delete source row
  delete from core.shows where id = source_show_id;
end $$;

grant execute on function core.merge_shows(uuid, uuid) to service_role;

-- ---------------------------------------------------------------------------
-- 3) Drop legacy cast tables
-- ---------------------------------------------------------------------------

drop view if exists core.v_episode_appearances;

drop table if exists core.episode_cast;
drop table if exists core.cast_memberships;
drop table if exists core.episode_appearances;
drop table if exists core.show_cast;

-- ---------------------------------------------------------------------------
-- 4) Recreate compatibility views (preserve legacy relation names as read-only views)
-- ---------------------------------------------------------------------------

create or replace view core.show_cast as
select
  show_name,
  cast_member_name,
  show_id,
  person_id,
  billing_order,
  role,
  credit_category,
  id,
  created_at,
  updated_at,
  source_type
from core.v_show_cast;

comment on view core.show_cast is
'Compatibility view: legacy show_cast table name backed by credits model (read-only).';

grant select on core.show_cast to anon, authenticated, service_role;

create or replace view core.episode_appearances as
select
  show_name,
  cast_member_name,
  seasons,
  tmdb_season_ids,
  tmdb_show_id,
  imdb_show_id,
  imdb_episode_title_ids,
  tmdb_episode_ids,
  total_episodes,
  show_id,
  person_id,
  id,
  created_at,
  updated_at
from core.v_episode_appearances_from_credits;

comment on view core.episode_appearances is
'Compatibility view: legacy episode_appearances table name backed by credits model (read-only).';

grant select on core.episode_appearances to anon, authenticated, service_role;

-- ---------------------------------------------------------------------------
-- 5) Recreate export view over credits model (preserve consumer name)
-- ---------------------------------------------------------------------------

create or replace view core.v_episode_appearances as
select
  show_name,
  cast_member_name,
  seasons,
  tmdb_season_ids,
  tmdb_show_id,
  imdb_show_id,
  imdb_episode_title_ids,
  tmdb_episode_ids,
  total_episodes
from core.v_episode_appearances_from_credits;

grant select on table core.v_episode_appearances to anon, authenticated, service_role;

commit;
