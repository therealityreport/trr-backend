-- Canonical migration re-asserting indexes applied live during Wave 2 FK
-- hardening on 2026-04-20 (pending rollout). CREATE INDEX IF NOT EXISTS is
-- a cheap catalog check when the index already exists, so this is safe to
-- replay on any environment including one that already ran the direct-psql
-- apply.
--
-- Non-concurrent form is intentional: this file is consumed by `supabase db
-- reset` against clean or test targets, where transactional migrations are
-- the expected idiom. Production already has these indexes from the
-- concurrent apply documented in docs/db/fk-index-hardening/wave-2-status.md.
--
-- Generator: stripped `CONCURRENTLY` from docs/db/fk-index-hardening/wave-2-forward.sql
-- (38 CREATE INDEX statements) and wrapped in a BEGIN/COMMIT block.

BEGIN;

CREATE INDEX IF NOT EXISTS "firebase_surveys_answers_option_id_idx" ON "firebase_surveys"."answers" USING btree ("option_id");
CREATE INDEX IF NOT EXISTS "firebase_surveys_answers_question_id_idx" ON "firebase_surveys"."answers" USING btree ("question_id");
CREATE INDEX IF NOT EXISTS "ml_analysis_media_assets_episode_id_idx" ON "ml"."analysis_media_assets" USING btree ("episode_id");
CREATE INDEX IF NOT EXISTS "ml_analysis_media_assets_media_asset_id_idx" ON "ml"."analysis_media_assets" USING btree ("media_asset_id") WHERE media_asset_id is not null;
CREATE INDEX IF NOT EXISTS "ml_analysis_media_assets_season_id_idx" ON "ml"."analysis_media_assets" USING btree ("season_id");
CREATE INDEX IF NOT EXISTS "ml_analysis_media_assets_show_id_idx" ON "ml"."analysis_media_assets" USING btree ("show_id");
CREATE INDEX IF NOT EXISTS "ml_analysis_media_cast_candidates_person_id_idx" ON "ml"."analysis_media_cast_candidates" USING btree ("person_id");
CREATE INDEX IF NOT EXISTS "ml_analysis_media_upload_sessions_episode_id_idx" ON "ml"."analysis_media_upload_sessions" USING btree ("episode_id") WHERE episode_id is not null;
CREATE INDEX IF NOT EXISTS "ml_analysis_media_upload_sessions_promoted_video_asset_id_idx" ON "ml"."analysis_media_upload_sessions" USING btree ("promoted_video_asset_id");
CREATE INDEX IF NOT EXISTS "ml_analysis_media_upload_sessions_season_id_idx" ON "ml"."analysis_media_upload_sessions" USING btree ("season_id");
CREATE INDEX IF NOT EXISTS "ml_analysis_media_upload_sessions_show_id_idx" ON "ml"."analysis_media_upload_sessions" USING btree ("show_id");
CREATE INDEX IF NOT EXISTS "ml_face_reference_images_duplicate_of_reference_image_id_idx" ON "ml"."face_reference_images" USING btree ("duplicate_of_reference_image_id");
CREATE INDEX IF NOT EXISTS "ml_face_reference_images_media_asset_id_idx" ON "ml"."face_reference_images" USING btree ("media_asset_id");
CREATE INDEX IF NOT EXISTS "ml_screentime_person_metrics_person_id_idx" ON "ml"."screentime_person_metrics" USING btree ("person_id");
CREATE INDEX IF NOT EXISTS "ml_screentime_reference_fingerprints_episode_id_idx" ON "ml"."screentime_reference_fingerprints" USING btree ("episode_id");
CREATE INDEX IF NOT EXISTS "ml_screentime_reference_fingerprints_run_id_idx" ON "ml"."screentime_reference_fingerprints" USING btree ("run_id");
CREATE INDEX IF NOT EXISTS "ml_screentime_reference_fingerprints_season_id_idx" ON "ml"."screentime_reference_fingerprints" USING btree ("season_id");
CREATE INDEX IF NOT EXISTS "ml_screentime_reference_fingerprints_show_id_idx" ON "ml"."screentime_reference_fingerprints" USING btree ("show_id");
CREATE INDEX IF NOT EXISTS "ml_screentime_reference_fingerprints_video_asset_id_idx" ON "ml"."screentime_reference_fingerprints" USING btree ("video_asset_id");
CREATE INDEX IF NOT EXISTS "ml_screentime_review_state_candidate_person_id_idx" ON "ml"."screentime_review_state" USING btree ("candidate_person_id");
CREATE INDEX IF NOT EXISTS "ml_screentime_review_state_episode_id_idx" ON "ml"."screentime_review_state" USING btree ("episode_id");
CREATE INDEX IF NOT EXISTS "ml_screentime_review_state_person_id_idx" ON "ml"."screentime_review_state" USING btree ("person_id");
CREATE INDEX IF NOT EXISTS "ml_screentime_review_state_season_id_idx" ON "ml"."screentime_review_state" USING btree ("season_id");
CREATE INDEX IF NOT EXISTS "ml_screentime_review_state_show_id_idx" ON "ml"."screentime_review_state" USING btree ("show_id");
CREATE INDEX IF NOT EXISTS "ml_screentime_review_state_video_asset_id_idx" ON "ml"."screentime_review_state" USING btree ("video_asset_id");
CREATE INDEX IF NOT EXISTS "ml_screentime_segments_person_id_idx" ON "ml"."screentime_segments" USING btree ("person_id");
CREATE INDEX IF NOT EXISTS "ml_screentime_unknown_clusters_assigned_person_id_idx" ON "ml"."screentime_unknown_clusters" USING btree ("assigned_person_id");
CREATE INDEX IF NOT EXISTS "screenalytics_face_bank_images_media_asset_id_idx" ON "screenalytics"."face_bank_images" USING btree ("media_asset_id");
CREATE INDEX IF NOT EXISTS "screenalytics_identity_locks_run_id_idx" ON "screenalytics"."identity_locks" USING btree ("run_id");
CREATE INDEX IF NOT EXISTS "screenalytics_media_upload_sessions_episode_id_idx" ON "screenalytics"."media_upload_sessions" USING btree ("episode_id");
CREATE INDEX IF NOT EXISTS "screenalytics_media_upload_sessions_promoted_video_asset_id_idx" ON "screenalytics"."media_upload_sessions" USING btree ("promoted_video_asset_id");
CREATE INDEX IF NOT EXISTS "screenalytics_media_upload_sessions_season_id_idx" ON "screenalytics"."media_upload_sessions" USING btree ("season_id");
CREATE INDEX IF NOT EXISTS "screenalytics_suggestion_applies_suggestion_id_idx" ON "screenalytics"."suggestion_applies" USING btree ("suggestion_id");
CREATE INDEX IF NOT EXISTS "screenalytics_suggestion_batches_run_id_idx" ON "screenalytics"."suggestion_batches" USING btree ("run_id");
CREATE INDEX IF NOT EXISTS "screenalytics_unknown_clusters_assigned_person_id_idx" ON "screenalytics"."unknown_clusters" USING btree ("assigned_person_id");
CREATE INDEX IF NOT EXISTS "screenalytics_video_assets_media_asset_id_idx" ON "screenalytics"."video_assets" USING btree ("media_asset_id") WHERE media_asset_id is not null;
CREATE INDEX IF NOT EXISTS "surveys_answers_survey_id_question_id_idx" ON "surveys"."answers" USING btree ("survey_id", "question_id");
CREATE INDEX IF NOT EXISTS "surveys_answers_survey_id_response_id_idx" ON "surveys"."answers" USING btree ("survey_id", "response_id");

COMMIT;
