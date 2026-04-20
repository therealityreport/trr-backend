-- wave-2 FK index hardening forward apply
-- generated_at: 2026-04-20T18:35:19Z
-- apply with direct Postgres connectivity only

-- Operator contract: set PGAPPNAME=fk-index-<wave>-apply before invoking psql.
-- This guard refuses to apply if the session is not running with that exact
-- application_name, which would indicate either an operator misconfiguration
-- or a pooler rewriting the connection.
DO $pre$
DECLARE
  app_name text;
BEGIN
  SELECT current_setting('application_name', true) INTO app_name;
  IF app_name IS NULL OR app_name NOT LIKE 'fk-index-%-apply' THEN
    RAISE EXCEPTION 'Refusing apply: application_name is %, expected fk-index-<wave>-apply. Set PGAPPNAME before running psql.',
      COALESCE(app_name, '<null>');
  END IF;
END
$pre$;

-- firebase_surveys.answers answers_option_id_fkey -> firebase_surveys_answers_option_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "firebase_surveys_answers_option_id_idx" ON "firebase_surveys"."answers" USING btree ("option_id");

-- firebase_surveys.answers answers_question_id_fkey -> firebase_surveys_answers_question_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "firebase_surveys_answers_question_id_idx" ON "firebase_surveys"."answers" USING btree ("question_id");

-- ml.analysis_media_assets analysis_media_assets_episode_id_fkey -> ml_analysis_media_assets_episode_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "ml_analysis_media_assets_episode_id_idx" ON "ml"."analysis_media_assets" USING btree ("episode_id");

-- ml.analysis_media_assets analysis_media_assets_media_asset_id_fkey -> ml_analysis_media_assets_media_asset_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "ml_analysis_media_assets_media_asset_id_idx" ON "ml"."analysis_media_assets" USING btree ("media_asset_id") WHERE media_asset_id is not null;

-- ml.analysis_media_assets analysis_media_assets_season_id_fkey -> ml_analysis_media_assets_season_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "ml_analysis_media_assets_season_id_idx" ON "ml"."analysis_media_assets" USING btree ("season_id");

-- ml.analysis_media_assets analysis_media_assets_show_id_fkey -> ml_analysis_media_assets_show_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "ml_analysis_media_assets_show_id_idx" ON "ml"."analysis_media_assets" USING btree ("show_id");

-- ml.analysis_media_cast_candidates analysis_media_cast_candidates_person_id_fkey -> ml_analysis_media_cast_candidates_person_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "ml_analysis_media_cast_candidates_person_id_idx" ON "ml"."analysis_media_cast_candidates" USING btree ("person_id");

-- ml.analysis_media_upload_sessions analysis_media_upload_sessions_episode_id_fkey -> ml_analysis_media_upload_sessions_episode_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "ml_analysis_media_upload_sessions_episode_id_idx" ON "ml"."analysis_media_upload_sessions" USING btree ("episode_id") WHERE episode_id is not null;

-- ml.analysis_media_upload_sessions analysis_media_upload_sessions_promoted_video_asset_id_fkey -> ml_analysis_media_upload_sessions_promoted_video_asset_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "ml_analysis_media_upload_sessions_promoted_video_asset_id_idx" ON "ml"."analysis_media_upload_sessions" USING btree ("promoted_video_asset_id");

-- ml.analysis_media_upload_sessions analysis_media_upload_sessions_season_id_fkey -> ml_analysis_media_upload_sessions_season_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "ml_analysis_media_upload_sessions_season_id_idx" ON "ml"."analysis_media_upload_sessions" USING btree ("season_id");

-- ml.analysis_media_upload_sessions analysis_media_upload_sessions_show_id_fkey -> ml_analysis_media_upload_sessions_show_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "ml_analysis_media_upload_sessions_show_id_idx" ON "ml"."analysis_media_upload_sessions" USING btree ("show_id");

-- ml.face_reference_images face_reference_images_duplicate_of_reference_image_id_fkey -> ml_face_reference_images_duplicate_of_reference_image_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "ml_face_reference_images_duplicate_of_reference_image_id_idx" ON "ml"."face_reference_images" USING btree ("duplicate_of_reference_image_id");

-- ml.face_reference_images face_reference_images_media_asset_id_fkey -> ml_face_reference_images_media_asset_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "ml_face_reference_images_media_asset_id_idx" ON "ml"."face_reference_images" USING btree ("media_asset_id");

-- ml.screentime_person_metrics screentime_person_metrics_person_id_fkey -> ml_screentime_person_metrics_person_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "ml_screentime_person_metrics_person_id_idx" ON "ml"."screentime_person_metrics" USING btree ("person_id");

-- ml.screentime_reference_fingerprints screentime_reference_fingerprints_episode_id_fkey -> ml_screentime_reference_fingerprints_episode_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "ml_screentime_reference_fingerprints_episode_id_idx" ON "ml"."screentime_reference_fingerprints" USING btree ("episode_id");

-- ml.screentime_reference_fingerprints screentime_reference_fingerprints_run_id_fkey -> ml_screentime_reference_fingerprints_run_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "ml_screentime_reference_fingerprints_run_id_idx" ON "ml"."screentime_reference_fingerprints" USING btree ("run_id");

-- ml.screentime_reference_fingerprints screentime_reference_fingerprints_season_id_fkey -> ml_screentime_reference_fingerprints_season_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "ml_screentime_reference_fingerprints_season_id_idx" ON "ml"."screentime_reference_fingerprints" USING btree ("season_id");

-- ml.screentime_reference_fingerprints screentime_reference_fingerprints_show_id_fkey -> ml_screentime_reference_fingerprints_show_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "ml_screentime_reference_fingerprints_show_id_idx" ON "ml"."screentime_reference_fingerprints" USING btree ("show_id");

-- ml.screentime_reference_fingerprints screentime_reference_fingerprints_video_asset_id_fkey -> ml_screentime_reference_fingerprints_video_asset_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "ml_screentime_reference_fingerprints_video_asset_id_idx" ON "ml"."screentime_reference_fingerprints" USING btree ("video_asset_id");

-- ml.screentime_review_state screentime_review_state_candidate_person_id_fkey -> ml_screentime_review_state_candidate_person_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "ml_screentime_review_state_candidate_person_id_idx" ON "ml"."screentime_review_state" USING btree ("candidate_person_id");

-- ml.screentime_review_state screentime_review_state_episode_id_fkey -> ml_screentime_review_state_episode_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "ml_screentime_review_state_episode_id_idx" ON "ml"."screentime_review_state" USING btree ("episode_id");

-- ml.screentime_review_state screentime_review_state_person_id_fkey -> ml_screentime_review_state_person_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "ml_screentime_review_state_person_id_idx" ON "ml"."screentime_review_state" USING btree ("person_id");

-- ml.screentime_review_state screentime_review_state_season_id_fkey -> ml_screentime_review_state_season_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "ml_screentime_review_state_season_id_idx" ON "ml"."screentime_review_state" USING btree ("season_id");

-- ml.screentime_review_state screentime_review_state_show_id_fkey -> ml_screentime_review_state_show_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "ml_screentime_review_state_show_id_idx" ON "ml"."screentime_review_state" USING btree ("show_id");

-- ml.screentime_review_state screentime_review_state_video_asset_id_fkey -> ml_screentime_review_state_video_asset_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "ml_screentime_review_state_video_asset_id_idx" ON "ml"."screentime_review_state" USING btree ("video_asset_id");

-- ml.screentime_segments screentime_segments_person_id_fkey -> ml_screentime_segments_person_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "ml_screentime_segments_person_id_idx" ON "ml"."screentime_segments" USING btree ("person_id");

-- ml.screentime_unknown_clusters screentime_unknown_clusters_assigned_person_id_fkey -> ml_screentime_unknown_clusters_assigned_person_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "ml_screentime_unknown_clusters_assigned_person_id_idx" ON "ml"."screentime_unknown_clusters" USING btree ("assigned_person_id");

-- screenalytics.face_bank_images face_bank_images_media_asset_id_fkey -> screenalytics_face_bank_images_media_asset_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "screenalytics_face_bank_images_media_asset_id_idx" ON "screenalytics"."face_bank_images" USING btree ("media_asset_id");

-- screenalytics.identity_locks identity_locks_run_id_fkey -> screenalytics_identity_locks_run_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "screenalytics_identity_locks_run_id_idx" ON "screenalytics"."identity_locks" USING btree ("run_id");

-- screenalytics.media_upload_sessions media_upload_sessions_episode_id_fkey -> screenalytics_media_upload_sessions_episode_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "screenalytics_media_upload_sessions_episode_id_idx" ON "screenalytics"."media_upload_sessions" USING btree ("episode_id");

-- screenalytics.media_upload_sessions media_upload_sessions_promoted_video_asset_id_fkey -> screenalytics_media_upload_sessions_promoted_video_asset_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "screenalytics_media_upload_sessions_promoted_video_asset_id_idx" ON "screenalytics"."media_upload_sessions" USING btree ("promoted_video_asset_id");

-- screenalytics.media_upload_sessions media_upload_sessions_season_id_fkey -> screenalytics_media_upload_sessions_season_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "screenalytics_media_upload_sessions_season_id_idx" ON "screenalytics"."media_upload_sessions" USING btree ("season_id");

-- screenalytics.suggestion_applies suggestion_applies_suggestion_id_fkey -> screenalytics_suggestion_applies_suggestion_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "screenalytics_suggestion_applies_suggestion_id_idx" ON "screenalytics"."suggestion_applies" USING btree ("suggestion_id");

-- screenalytics.suggestion_batches suggestion_batches_run_id_fkey -> screenalytics_suggestion_batches_run_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "screenalytics_suggestion_batches_run_id_idx" ON "screenalytics"."suggestion_batches" USING btree ("run_id");

-- screenalytics.unknown_clusters unknown_clusters_assigned_person_id_fkey -> screenalytics_unknown_clusters_assigned_person_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "screenalytics_unknown_clusters_assigned_person_id_idx" ON "screenalytics"."unknown_clusters" USING btree ("assigned_person_id");

-- screenalytics.video_assets video_assets_media_asset_id_fkey -> screenalytics_video_assets_media_asset_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "screenalytics_video_assets_media_asset_id_idx" ON "screenalytics"."video_assets" USING btree ("media_asset_id") WHERE media_asset_id is not null;

-- surveys.answers survey_answers_survey_id_question_id_fkey -> surveys_answers_survey_id_question_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "surveys_answers_survey_id_question_id_idx" ON "surveys"."answers" USING btree ("survey_id", "question_id");

-- surveys.answers survey_answers_survey_id_response_id_fkey -> surveys_answers_survey_id_response_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "surveys_answers_survey_id_response_id_idx" ON "surveys"."answers" USING btree ("survey_id", "response_id");
