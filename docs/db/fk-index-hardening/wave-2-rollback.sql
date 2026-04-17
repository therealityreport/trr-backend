-- wave-2 FK index hardening rollback
-- generated_at: 2026-04-17T19:09:05Z
-- apply with direct Postgres connectivity only

-- rollback firebase_surveys.answers firebase_surveys_answers_option_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "firebase_surveys"."firebase_surveys_answers_option_id_idx";

-- rollback firebase_surveys.answers firebase_surveys_answers_question_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "firebase_surveys"."firebase_surveys_answers_question_id_idx";

-- rollback ml.analysis_media_assets ml_analysis_media_assets_episode_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "ml"."ml_analysis_media_assets_episode_id_idx";

-- rollback ml.analysis_media_assets ml_analysis_media_assets_media_asset_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "ml"."ml_analysis_media_assets_media_asset_id_idx";

-- rollback ml.analysis_media_assets ml_analysis_media_assets_season_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "ml"."ml_analysis_media_assets_season_id_idx";

-- rollback ml.analysis_media_assets ml_analysis_media_assets_show_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "ml"."ml_analysis_media_assets_show_id_idx";

-- rollback ml.analysis_media_cast_candidates ml_analysis_media_cast_candidates_person_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "ml"."ml_analysis_media_cast_candidates_person_id_idx";

-- rollback ml.analysis_media_upload_sessions ml_analysis_media_upload_sessions_episode_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "ml"."ml_analysis_media_upload_sessions_episode_id_idx";

-- rollback ml.analysis_media_upload_sessions ml_analysis_media_upload_sessions_promoted_video_asset_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "ml"."ml_analysis_media_upload_sessions_promoted_video_asset_id_idx";

-- rollback ml.analysis_media_upload_sessions ml_analysis_media_upload_sessions_season_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "ml"."ml_analysis_media_upload_sessions_season_id_idx";

-- rollback ml.analysis_media_upload_sessions ml_analysis_media_upload_sessions_show_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "ml"."ml_analysis_media_upload_sessions_show_id_idx";

-- rollback ml.face_reference_images ml_face_reference_images_duplicate_of_reference_image_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "ml"."ml_face_reference_images_duplicate_of_reference_image_id_idx";

-- rollback ml.face_reference_images ml_face_reference_images_media_asset_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "ml"."ml_face_reference_images_media_asset_id_idx";

-- rollback ml.screentime_person_metrics ml_screentime_person_metrics_person_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "ml"."ml_screentime_person_metrics_person_id_idx";

-- rollback ml.screentime_reference_fingerprints ml_screentime_reference_fingerprints_episode_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "ml"."ml_screentime_reference_fingerprints_episode_id_idx";

-- rollback ml.screentime_reference_fingerprints ml_screentime_reference_fingerprints_run_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "ml"."ml_screentime_reference_fingerprints_run_id_idx";

-- rollback ml.screentime_reference_fingerprints ml_screentime_reference_fingerprints_season_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "ml"."ml_screentime_reference_fingerprints_season_id_idx";

-- rollback ml.screentime_reference_fingerprints ml_screentime_reference_fingerprints_show_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "ml"."ml_screentime_reference_fingerprints_show_id_idx";

-- rollback ml.screentime_reference_fingerprints ml_screentime_reference_fingerprints_video_asset_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "ml"."ml_screentime_reference_fingerprints_video_asset_id_idx";

-- rollback ml.screentime_review_state ml_screentime_review_state_candidate_person_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "ml"."ml_screentime_review_state_candidate_person_id_idx";

-- rollback ml.screentime_review_state ml_screentime_review_state_episode_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "ml"."ml_screentime_review_state_episode_id_idx";

-- rollback ml.screentime_review_state ml_screentime_review_state_person_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "ml"."ml_screentime_review_state_person_id_idx";

-- rollback ml.screentime_review_state ml_screentime_review_state_season_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "ml"."ml_screentime_review_state_season_id_idx";

-- rollback ml.screentime_review_state ml_screentime_review_state_show_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "ml"."ml_screentime_review_state_show_id_idx";

-- rollback ml.screentime_review_state ml_screentime_review_state_video_asset_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "ml"."ml_screentime_review_state_video_asset_id_idx";

-- rollback ml.screentime_segments ml_screentime_segments_person_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "ml"."ml_screentime_segments_person_id_idx";

-- rollback ml.screentime_unknown_clusters ml_screentime_unknown_clusters_assigned_person_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "ml"."ml_screentime_unknown_clusters_assigned_person_id_idx";

-- rollback screenalytics.face_bank_images screenalytics_face_bank_images_media_asset_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "screenalytics"."screenalytics_face_bank_images_media_asset_id_idx";

-- rollback screenalytics.identity_locks screenalytics_identity_locks_run_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "screenalytics"."screenalytics_identity_locks_run_id_idx";

-- rollback screenalytics.media_upload_sessions screenalytics_media_upload_sessions_episode_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "screenalytics"."screenalytics_media_upload_sessions_episode_id_idx";

-- rollback screenalytics.media_upload_sessions screenalytics_media_upload_sessions_promoted_video_asset_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "screenalytics"."screenalytics_media_upload_sessions_promoted_video_asset_id_idx";

-- rollback screenalytics.media_upload_sessions screenalytics_media_upload_sessions_season_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "screenalytics"."screenalytics_media_upload_sessions_season_id_idx";

-- rollback screenalytics.suggestion_applies screenalytics_suggestion_applies_suggestion_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "screenalytics"."screenalytics_suggestion_applies_suggestion_id_idx";

-- rollback screenalytics.suggestion_batches screenalytics_suggestion_batches_run_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "screenalytics"."screenalytics_suggestion_batches_run_id_idx";

-- rollback screenalytics.unknown_clusters screenalytics_unknown_clusters_assigned_person_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "screenalytics"."screenalytics_unknown_clusters_assigned_person_id_idx";

-- rollback screenalytics.video_assets screenalytics_video_assets_media_asset_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "screenalytics"."screenalytics_video_assets_media_asset_id_idx";

-- rollback surveys.answers surveys_answers_survey_id_question_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "surveys"."surveys_answers_survey_id_question_id_idx";

-- rollback surveys.answers surveys_answers_survey_id_response_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "surveys"."surveys_answers_survey_id_response_id_idx";
