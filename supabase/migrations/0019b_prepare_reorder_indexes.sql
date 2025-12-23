begin;

-- Rename indexes that would conflict with 0020_reorder_show_tables.sql.
do $$
declare
  rec record;
  new_qualified text;
begin
  for rec in
    select * from (values
      ('core.shows_pkey', 'shows_legacy_pkey'),
      ('core.seasons_pkey', 'seasons_legacy_pkey'),
      ('core.episodes_pkey', 'episodes_legacy_pkey'),
      ('core.core_shows_external_ids_gin', 'core_shows_external_ids_gin_legacy'),
      ('core.core_shows_external_ids_imdb_unique', 'core_shows_external_ids_imdb_unique_legacy'),
      ('core.core_shows_external_ids_tmdb_unique', 'core_shows_external_ids_tmdb_unique_legacy'),
      ('core.seasons_show_id_idx', 'seasons_show_id_idx_legacy'),
      ('core.core_seasons_show_id_season_number_idx', 'core_seasons_show_id_season_number_idx_legacy'),
      ('core.core_seasons_tmdb_series_season_unique', 'core_seasons_tmdb_series_season_unique_legacy'),
      ('core.seasons_show_id_season_number_unique', 'seasons_show_id_season_number_unique_legacy'),
      ('core.episodes_season_id_idx', 'episodes_season_id_idx_legacy'),
      ('core.core_episodes_show_season_idx', 'core_episodes_show_season_idx_legacy'),
      ('core.core_episodes_show_season_episode_unique', 'core_episodes_show_season_episode_unique_legacy'),
      ('core.core_episodes_imdb_episode_id_unique', 'core_episodes_imdb_episode_id_unique_legacy'),
      ('core.core_episodes_tmdb_episode_id_unique', 'core_episodes_tmdb_episode_id_unique_legacy'),
      ('core.episodes_season_id_episode_number_unique', 'episodes_season_id_episode_number_unique_legacy')
    ) as t(old_name, new_name)
  loop
    new_qualified := format('core.%s', rec.new_name);
    if to_regclass(rec.old_name) is not null and to_regclass(new_qualified) is null then
      execute format('alter index %s rename to %I', rec.old_name, rec.new_name);
    end if;
  end loop;
end $$;

commit;
