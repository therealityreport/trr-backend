-- Canonical migration re-asserting indexes applied live during Wave 1 FK
-- hardening on 2026-04-20 (pending rollout). CREATE INDEX IF NOT EXISTS is
-- a cheap catalog check when the index already exists, so this is safe to
-- replay on any environment including one that already ran the direct-psql
-- apply.
--
-- Non-concurrent form is intentional: this file is consumed by `supabase db
-- reset` against clean or test targets, where transactional migrations are
-- the expected idiom. Production already has these indexes from the
-- concurrent apply documented in docs/db/fk-index-hardening/wave-1-status.md.
--
-- Generator: stripped `CONCURRENTLY` from docs/db/fk-index-hardening/wave-1-forward.sql
-- (51 CREATE INDEX statements) and wrapped in a BEGIN/COMMIT block.

BEGIN;

CREATE INDEX IF NOT EXISTS "admin_recent_people_views_person_id_idx" ON "admin"."recent_people_views" USING btree ("person_id");
CREATE INDEX IF NOT EXISTS "social_account_hashtag_assignments_season_id_idx" ON "social"."account_hashtag_assignments" USING btree ("season_id");
CREATE INDEX IF NOT EXISTS "social_account_hashtag_review_queue_resolved_season_id_idx" ON "social"."account_hashtag_review_queue" USING btree ("resolved_season_id") WHERE resolved_season_id is not null;
CREATE INDEX IF NOT EXISTS "social_account_hashtag_review_queue_resolved_show_id_idx" ON "social"."account_hashtag_review_queue" USING btree ("resolved_show_id") WHERE resolved_show_id is not null;
CREATE INDEX IF NOT EXISTS "social_dm_messages_sender_id_idx" ON "social"."dm_messages" USING btree ("sender_id");
CREATE INDEX IF NOT EXISTS "social_dm_read_receipts_last_read_message_id_idx" ON "social"."dm_read_receipts" USING btree ("last_read_message_id");
CREATE INDEX IF NOT EXISTS "social_dm_read_receipts_user_id_idx" ON "social"."dm_read_receipts" USING btree ("user_id");
CREATE INDEX IF NOT EXISTS "social_facebook_account_catalog_posts_assigned_season_id_idx" ON "social"."facebook_account_catalog_posts" USING btree ("assigned_season_id");
CREATE INDEX IF NOT EXISTS "social_facebook_account_catalog_posts_assigned_show_id_idx" ON "social"."facebook_account_catalog_posts" USING btree ("assigned_show_id");
CREATE INDEX IF NOT EXISTS "social_facebook_account_catalog_posts_last_backfill_run_id_idx" ON "social"."facebook_account_catalog_posts" USING btree ("last_backfill_run_id");
CREATE INDEX IF NOT EXISTS "social_instagram_account_catalog_posts_assigned_season_id_idx" ON "social"."instagram_account_catalog_posts" USING btree ("assigned_season_id") WHERE assigned_season_id is not null;
CREATE INDEX IF NOT EXISTS "social_instagram_account_catalog_posts_assigned_show_id_idx" ON "social"."instagram_account_catalog_posts" USING btree ("assigned_show_id") WHERE assigned_show_id is not null;
CREATE INDEX IF NOT EXISTS "social_instagram_account_catalog_posts_last_backfill_run_id_idx" ON "social"."instagram_account_catalog_posts" USING btree ("last_backfill_run_id") WHERE last_backfill_run_id is not null;
CREATE INDEX IF NOT EXISTS "social_instagram_comments_last_seen_run_id_idx" ON "social"."instagram_comments" USING btree ("last_seen_run_id");
CREATE INDEX IF NOT EXISTS "social_reactions_user_id_idx" ON "social"."reactions" USING btree ("user_id");
CREATE INDEX IF NOT EXISTS "social_reddit_period_post_matches_reddit_post_id_idx" ON "social"."reddit_period_post_matches" USING btree ("reddit_post_id");
CREATE INDEX IF NOT EXISTS "social_reddit_period_post_matches_run_id_idx" ON "social"."reddit_period_post_matches" USING btree ("run_id");
CREATE INDEX IF NOT EXISTS "social_reddit_period_post_matches_season_id_idx" ON "social"."reddit_period_post_matches" USING btree ("season_id");
CREATE INDEX IF NOT EXISTS "social_reddit_refresh_runs_season_id_idx" ON "social"."reddit_refresh_runs" USING btree ("season_id");
CREATE INDEX IF NOT EXISTS "social_scrape_jobs_person_id_idx" ON "social"."scrape_jobs" USING btree ("person_id") WHERE person_id is not null;
CREATE INDEX IF NOT EXISTS "social_scrape_jobs_show_id_idx" ON "social"."scrape_jobs" USING btree ("show_id");
CREATE INDEX IF NOT EXISTS "social_scrape_runs_show_id_idx" ON "social"."scrape_runs" USING btree ("show_id");
CREATE INDEX IF NOT EXISTS "social_scrape_workers_current_job_id_idx" ON "social"."scrape_workers" USING btree ("current_job_id") WHERE current_job_id is not null;
CREATE INDEX IF NOT EXISTS "social_scrape_workers_run_id_idx" ON "social"."scrape_workers" USING btree ("run_id") WHERE run_id is not null;
CREATE INDEX IF NOT EXISTS "social_shared_account_run_partitions_job_id_idx" ON "social"."shared_account_run_partitions" USING btree ("job_id");
CREATE INDEX IF NOT EXISTS "social_shared_account_sources_last_scrape_job_id_idx" ON "social"."shared_account_sources" USING btree ("last_scrape_job_id") WHERE last_scrape_job_id is not null;
CREATE INDEX IF NOT EXISTS "social_shared_account_sources_last_scrape_run_id_idx" ON "social"."shared_account_sources" USING btree ("last_scrape_run_id") WHERE last_scrape_run_id is not null;
CREATE INDEX IF NOT EXISTS "social_shared_post_matches_matched_show_id_idx" ON "social"."shared_post_matches" USING btree ("matched_show_id");
CREATE INDEX IF NOT EXISTS "social_shared_post_matches_run_id_idx" ON "social"."shared_post_matches" USING btree ("run_id");
CREATE INDEX IF NOT EXISTS "social_shared_post_review_queue_resolved_show_id_idx" ON "social"."shared_post_review_queue" USING btree ("resolved_show_id");
CREATE INDEX IF NOT EXISTS "social_shared_post_review_queue_run_id_idx" ON "social"."shared_post_review_queue" USING btree ("run_id");
CREATE INDEX IF NOT EXISTS "social_sync_sessions_show_id_idx" ON "social"."sync_sessions" USING btree ("show_id");
CREATE INDEX IF NOT EXISTS "social_threads_account_catalog_posts_assigned_season_id_idx" ON "social"."threads_account_catalog_posts" USING btree ("assigned_season_id");
CREATE INDEX IF NOT EXISTS "social_threads_account_catalog_posts_assigned_show_id_idx" ON "social"."threads_account_catalog_posts" USING btree ("assigned_show_id");
CREATE INDEX IF NOT EXISTS "social_threads_account_catalog_posts_last_backfill_run_id_idx" ON "social"."threads_account_catalog_posts" USING btree ("last_backfill_run_id");
CREATE INDEX IF NOT EXISTS "social_tiktok_account_catalog_posts_assigned_season_id_idx" ON "social"."tiktok_account_catalog_posts" USING btree ("assigned_season_id") WHERE assigned_season_id is not null;
CREATE INDEX IF NOT EXISTS "social_tiktok_account_catalog_posts_assigned_show_id_idx" ON "social"."tiktok_account_catalog_posts" USING btree ("assigned_show_id") WHERE assigned_show_id is not null;
CREATE INDEX IF NOT EXISTS "social_tiktok_account_catalog_posts_last_backfill_run_id_idx" ON "social"."tiktok_account_catalog_posts" USING btree ("last_backfill_run_id");
CREATE INDEX IF NOT EXISTS "social_tiktok_anomaly_events_post_id_idx" ON "social"."tiktok_anomaly_events" USING btree ("post_id");
CREATE INDEX IF NOT EXISTS "social_tiktok_comments_last_seen_run_id_idx" ON "social"."tiktok_comments" USING btree ("last_seen_run_id");
CREATE INDEX IF NOT EXISTS "social_tiktok_post_cast_members_run_id_idx" ON "social"."tiktok_post_cast_members" USING btree ("run_id");
CREATE INDEX IF NOT EXISTS "social_twitter_account_catalog_posts_assigned_season_id_idx" ON "social"."twitter_account_catalog_posts" USING btree ("assigned_season_id") WHERE assigned_season_id is not null;
CREATE INDEX IF NOT EXISTS "social_twitter_account_catalog_posts_assigned_show_id_idx" ON "social"."twitter_account_catalog_posts" USING btree ("assigned_show_id") WHERE assigned_show_id is not null;
CREATE INDEX IF NOT EXISTS "social_twitter_account_catalog_posts_last_backfill_run_id_idx" ON "social"."twitter_account_catalog_posts" USING btree ("last_backfill_run_id");
CREATE INDEX IF NOT EXISTS "social_twitter_tweets_last_seen_run_id_idx" ON "social"."twitter_tweets" USING btree ("last_seen_run_id") WHERE last_seen_run_id is not null;
CREATE INDEX IF NOT EXISTS "social_youtube_account_catalog_posts_assigned_season_id_idx" ON "social"."youtube_account_catalog_posts" USING btree ("assigned_season_id") WHERE assigned_season_id is not null;
CREATE INDEX IF NOT EXISTS "social_youtube_account_catalog_posts_assigned_show_id_idx" ON "social"."youtube_account_catalog_posts" USING btree ("assigned_show_id") WHERE assigned_show_id is not null;
CREATE INDEX IF NOT EXISTS "social_youtube_account_catalog_posts_last_backfill_run_id_idx" ON "social"."youtube_account_catalog_posts" USING btree ("last_backfill_run_id");
CREATE INDEX IF NOT EXISTS "social_youtube_channel_sync_state_show_id_idx" ON "social"."youtube_channel_sync_state" USING btree ("show_id");
CREATE INDEX IF NOT EXISTS "social_youtube_comments_last_seen_run_id_idx" ON "social"."youtube_comments" USING btree ("last_seen_run_id");
CREATE INDEX IF NOT EXISTS "social_youtube_video_sync_state_show_id_idx" ON "social"."youtube_video_sync_state" USING btree ("show_id");

COMMIT;
