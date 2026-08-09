-- Cover watcher foreign keys used during parent deletion checks and joins.
-- This is additive follow-up hardening for the initial watcher migration.

create index if not exists show_season_media_watches_season_identity_idx
  on core.show_season_media_watches (season_id, show_id, target_season_number);

create index if not exists show_season_media_watch_runs_baseline_generation_idx
  on core.show_season_media_watch_runs (baseline_generation_id)
  where baseline_generation_id is not null;

create index if not exists show_season_media_watch_runs_bravo_run_idx
  on core.show_season_media_watch_runs (bravotv_image_run_id)
  where bravotv_image_run_id is not null;

create index if not exists show_season_media_watch_observations_baseline_generation_idx
  on core.show_season_media_watch_observations (baseline_generation_id)
  where baseline_generation_id is not null;
