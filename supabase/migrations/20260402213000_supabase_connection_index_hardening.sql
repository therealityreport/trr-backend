begin;

create index if not exists core_season_images_show_id_idx
  on core.season_images(show_id);

create index if not exists core_season_images_show_season_hosted_idx
  on core.season_images(show_id, season_number)
  where hosted_url is not null;

create index if not exists admin_brand_family_wikipedia_show_links_matched_show_id_idx
  on admin.brand_family_wikipedia_show_links(matched_show_id);

create index if not exists core_show_cast_role_assignments_person_id_idx
  on core.show_cast_role_assignments(person_id);

create index if not exists core_show_cast_role_assignments_season_id_idx
  on core.show_cast_role_assignments(season_id)
  where season_id is not null;

create index if not exists core_show_source_latest_source_id_idx
  on core.show_source_latest(source_id);

create index if not exists core_show_source_history_source_id_idx
  on core.show_source_history(source_id);

create index if not exists core_season_source_latest_source_id_idx
  on core.season_source_latest(source_id);

create index if not exists core_season_source_history_source_id_idx
  on core.season_source_history(source_id);

create index if not exists core_episode_source_latest_source_id_idx
  on core.episode_source_latest(source_id);

create index if not exists core_episode_source_history_source_id_idx
  on core.episode_source_history(source_id);

create index if not exists core_person_source_latest_source_id_idx
  on core.person_source_latest(source_id);

create index if not exists core_person_source_history_source_id_idx
  on core.person_source_history(source_id);

create index if not exists core_media_uploads_media_asset_id_idx
  on core.media_uploads(media_asset_id)
  where media_asset_id is not null;

create index if not exists core_media_uploads_media_link_id_idx
  on core.media_uploads(media_link_id)
  where media_link_id is not null;

create index if not exists public_surveys_current_episode_id_idx
  on public.surveys(current_episode_id)
  where current_episode_id is not null;

create index if not exists core_shows_primary_backdrop_image_id_idx
  on core.shows(primary_backdrop_image_id)
  where primary_backdrop_image_id is not null;

create index if not exists core_shows_primary_logo_image_id_idx
  on core.shows(primary_logo_image_id)
  where primary_logo_image_id is not null;

create index if not exists core_shows_primary_poster_image_id_idx
  on core.shows(primary_poster_image_id)
  where primary_poster_image_id is not null;

create index if not exists screenalytics_cast_screentime_reference_fingerprints_episode_id_idx
  on screenalytics.cast_screentime_reference_fingerprints(episode_id);

create index if not exists screenalytics_cast_screentime_reference_fingerprints_run_id_idx
  on screenalytics.cast_screentime_reference_fingerprints(run_id);

create index if not exists screenalytics_cast_screentime_reference_fingerprints_season_id_idx
  on screenalytics.cast_screentime_reference_fingerprints(season_id)
  where season_id is not null;

create index if not exists screenalytics_cast_screentime_suggestion_decisions_episode_id_idx
  on screenalytics.cast_screentime_suggestion_decisions(episode_id)
  where episode_id is not null;

create index if not exists screenalytics_cast_screentime_suggestion_decisions_person_id_idx
  on screenalytics.cast_screentime_suggestion_decisions(person_id);

create index if not exists screenalytics_cast_screentime_suggestion_decisions_season_id_idx
  on screenalytics.cast_screentime_suggestion_decisions(season_id)
  where season_id is not null;

create index if not exists screenalytics_cast_screentime_suggestion_decisions_video_asset_id_idx
  on screenalytics.cast_screentime_suggestion_decisions(video_asset_id);

create index if not exists screenalytics_cast_screentime_unknown_review_state_candidate_person_id_idx
  on screenalytics.cast_screentime_unknown_review_state(candidate_person_id)
  where candidate_person_id is not null;

create index if not exists screenalytics_cast_screentime_unknown_review_state_episode_id_idx
  on screenalytics.cast_screentime_unknown_review_state(episode_id)
  where episode_id is not null;

create index if not exists screenalytics_cast_screentime_unknown_review_state_season_id_idx
  on screenalytics.cast_screentime_unknown_review_state(season_id)
  where season_id is not null;

create index if not exists screenalytics_cast_screentime_unknown_review_state_video_asset_id_idx
  on screenalytics.cast_screentime_unknown_review_state(video_asset_id);

drop index if exists core.media_links_one_primary_uq;
drop index if exists core.show_images_source_unique;

commit;
