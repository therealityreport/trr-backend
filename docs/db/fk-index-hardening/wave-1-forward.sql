-- wave-1 FK index hardening forward apply
-- generated_at: 2026-04-17T19:09:04Z
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

-- admin.recent_people_views recent_people_views_person_id_fkey -> admin_recent_people_views_person_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "admin_recent_people_views_person_id_idx" ON "admin"."recent_people_views" USING btree ("person_id");

-- social.account_hashtag_assignments account_hashtag_assignments_season_id_fkey -> social_account_hashtag_assignments_season_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "social_account_hashtag_assignments_season_id_idx" ON "social"."account_hashtag_assignments" USING btree ("season_id");

-- social.account_hashtag_review_queue account_hashtag_review_queue_resolved_season_id_fkey -> social_account_hashtag_review_queue_resolved_season_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "social_account_hashtag_review_queue_resolved_season_id_idx" ON "social"."account_hashtag_review_queue" USING btree ("resolved_season_id") WHERE resolved_season_id is not null;

-- social.account_hashtag_review_queue account_hashtag_review_queue_resolved_show_id_fkey -> social_account_hashtag_review_queue_resolved_show_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "social_account_hashtag_review_queue_resolved_show_id_idx" ON "social"."account_hashtag_review_queue" USING btree ("resolved_show_id") WHERE resolved_show_id is not null;

-- social.dm_messages dm_messages_sender_id_fkey -> social_dm_messages_sender_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "social_dm_messages_sender_id_idx" ON "social"."dm_messages" USING btree ("sender_id");

-- social.dm_read_receipts dm_read_receipts_last_read_message_id_fkey -> social_dm_read_receipts_last_read_message_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "social_dm_read_receipts_last_read_message_id_idx" ON "social"."dm_read_receipts" USING btree ("last_read_message_id");

-- social.dm_read_receipts dm_read_receipts_user_id_fkey -> social_dm_read_receipts_user_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "social_dm_read_receipts_user_id_idx" ON "social"."dm_read_receipts" USING btree ("user_id");

-- social.facebook_account_catalog_posts facebook_account_catalog_posts_assigned_season_id_fkey -> social_facebook_account_catalog_posts_assigned_season_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "social_facebook_account_catalog_posts_assigned_season_id_idx" ON "social"."facebook_account_catalog_posts" USING btree ("assigned_season_id");

-- social.facebook_account_catalog_posts facebook_account_catalog_posts_assigned_show_id_fkey -> social_facebook_account_catalog_posts_assigned_show_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "social_facebook_account_catalog_posts_assigned_show_id_idx" ON "social"."facebook_account_catalog_posts" USING btree ("assigned_show_id");

-- social.facebook_account_catalog_posts facebook_account_catalog_posts_last_backfill_run_id_fkey -> social_facebook_account_catalog_posts_last_backfill_run_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "social_facebook_account_catalog_posts_last_backfill_run_id_idx" ON "social"."facebook_account_catalog_posts" USING btree ("last_backfill_run_id");

-- social.instagram_account_catalog_posts instagram_account_catalog_posts_assigned_season_id_fkey -> social_instagram_account_catalog_posts_assigned_season_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "social_instagram_account_catalog_posts_assigned_season_id_idx" ON "social"."instagram_account_catalog_posts" USING btree ("assigned_season_id") WHERE assigned_season_id is not null;

-- social.instagram_account_catalog_posts instagram_account_catalog_posts_assigned_show_id_fkey -> social_instagram_account_catalog_posts_assigned_show_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "social_instagram_account_catalog_posts_assigned_show_id_idx" ON "social"."instagram_account_catalog_posts" USING btree ("assigned_show_id") WHERE assigned_show_id is not null;

-- social.instagram_account_catalog_posts instagram_account_catalog_posts_last_backfill_run_id_fkey -> social_instagram_account_catalog_posts_last_backfill_run_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "social_instagram_account_catalog_posts_last_backfill_run_id_idx" ON "social"."instagram_account_catalog_posts" USING btree ("last_backfill_run_id") WHERE last_backfill_run_id is not null;

-- social.instagram_comments instagram_comments_last_seen_run_id_fkey -> social_instagram_comments_last_seen_run_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "social_instagram_comments_last_seen_run_id_idx" ON "social"."instagram_comments" USING btree ("last_seen_run_id");

-- social.reactions reactions_user_id_fkey -> social_reactions_user_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "social_reactions_user_id_idx" ON "social"."reactions" USING btree ("user_id");

-- social.reddit_period_post_matches reddit_period_post_matches_reddit_post_id_fkey -> social_reddit_period_post_matches_reddit_post_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "social_reddit_period_post_matches_reddit_post_id_idx" ON "social"."reddit_period_post_matches" USING btree ("reddit_post_id");

-- social.reddit_period_post_matches reddit_period_post_matches_run_id_fkey -> social_reddit_period_post_matches_run_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "social_reddit_period_post_matches_run_id_idx" ON "social"."reddit_period_post_matches" USING btree ("run_id");

-- social.reddit_period_post_matches reddit_period_post_matches_season_id_fkey -> social_reddit_period_post_matches_season_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "social_reddit_period_post_matches_season_id_idx" ON "social"."reddit_period_post_matches" USING btree ("season_id");

-- social.reddit_refresh_runs reddit_refresh_runs_season_id_fkey -> social_reddit_refresh_runs_season_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "social_reddit_refresh_runs_season_id_idx" ON "social"."reddit_refresh_runs" USING btree ("season_id");

-- social.scrape_jobs scrape_jobs_person_id_fkey -> social_scrape_jobs_person_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "social_scrape_jobs_person_id_idx" ON "social"."scrape_jobs" USING btree ("person_id") WHERE person_id is not null;

-- social.scrape_jobs scrape_jobs_show_id_fkey -> social_scrape_jobs_show_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "social_scrape_jobs_show_id_idx" ON "social"."scrape_jobs" USING btree ("show_id");

-- social.scrape_runs scrape_runs_show_id_fkey -> social_scrape_runs_show_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "social_scrape_runs_show_id_idx" ON "social"."scrape_runs" USING btree ("show_id");

-- social.scrape_workers scrape_workers_current_job_id_fkey -> social_scrape_workers_current_job_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "social_scrape_workers_current_job_id_idx" ON "social"."scrape_workers" USING btree ("current_job_id") WHERE current_job_id is not null;

-- social.scrape_workers scrape_workers_run_id_fkey -> social_scrape_workers_run_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "social_scrape_workers_run_id_idx" ON "social"."scrape_workers" USING btree ("run_id") WHERE run_id is not null;

-- social.shared_account_run_partitions shared_account_run_partitions_job_id_fkey -> social_shared_account_run_partitions_job_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "social_shared_account_run_partitions_job_id_idx" ON "social"."shared_account_run_partitions" USING btree ("job_id");

-- social.shared_account_sources shared_account_sources_last_scrape_job_id_fkey -> social_shared_account_sources_last_scrape_job_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "social_shared_account_sources_last_scrape_job_id_idx" ON "social"."shared_account_sources" USING btree ("last_scrape_job_id") WHERE last_scrape_job_id is not null;

-- social.shared_account_sources shared_account_sources_last_scrape_run_id_fkey -> social_shared_account_sources_last_scrape_run_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "social_shared_account_sources_last_scrape_run_id_idx" ON "social"."shared_account_sources" USING btree ("last_scrape_run_id") WHERE last_scrape_run_id is not null;

-- social.shared_post_matches shared_post_matches_matched_show_id_fkey -> social_shared_post_matches_matched_show_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "social_shared_post_matches_matched_show_id_idx" ON "social"."shared_post_matches" USING btree ("matched_show_id");

-- social.shared_post_matches shared_post_matches_run_id_fkey -> social_shared_post_matches_run_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "social_shared_post_matches_run_id_idx" ON "social"."shared_post_matches" USING btree ("run_id");

-- social.shared_post_review_queue shared_post_review_queue_resolved_show_id_fkey -> social_shared_post_review_queue_resolved_show_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "social_shared_post_review_queue_resolved_show_id_idx" ON "social"."shared_post_review_queue" USING btree ("resolved_show_id");

-- social.shared_post_review_queue shared_post_review_queue_run_id_fkey -> social_shared_post_review_queue_run_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "social_shared_post_review_queue_run_id_idx" ON "social"."shared_post_review_queue" USING btree ("run_id");

-- social.sync_sessions sync_sessions_show_id_fkey -> social_sync_sessions_show_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "social_sync_sessions_show_id_idx" ON "social"."sync_sessions" USING btree ("show_id");

-- social.threads_account_catalog_posts threads_account_catalog_posts_assigned_season_id_fkey -> social_threads_account_catalog_posts_assigned_season_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "social_threads_account_catalog_posts_assigned_season_id_idx" ON "social"."threads_account_catalog_posts" USING btree ("assigned_season_id");

-- social.threads_account_catalog_posts threads_account_catalog_posts_assigned_show_id_fkey -> social_threads_account_catalog_posts_assigned_show_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "social_threads_account_catalog_posts_assigned_show_id_idx" ON "social"."threads_account_catalog_posts" USING btree ("assigned_show_id");

-- social.threads_account_catalog_posts threads_account_catalog_posts_last_backfill_run_id_fkey -> social_threads_account_catalog_posts_last_backfill_run_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "social_threads_account_catalog_posts_last_backfill_run_id_idx" ON "social"."threads_account_catalog_posts" USING btree ("last_backfill_run_id");

-- social.tiktok_account_catalog_posts tiktok_account_catalog_posts_assigned_season_id_fkey -> social_tiktok_account_catalog_posts_assigned_season_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "social_tiktok_account_catalog_posts_assigned_season_id_idx" ON "social"."tiktok_account_catalog_posts" USING btree ("assigned_season_id") WHERE assigned_season_id is not null;

-- social.tiktok_account_catalog_posts tiktok_account_catalog_posts_assigned_show_id_fkey -> social_tiktok_account_catalog_posts_assigned_show_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "social_tiktok_account_catalog_posts_assigned_show_id_idx" ON "social"."tiktok_account_catalog_posts" USING btree ("assigned_show_id") WHERE assigned_show_id is not null;

-- social.tiktok_account_catalog_posts tiktok_account_catalog_posts_last_backfill_run_id_fkey -> social_tiktok_account_catalog_posts_last_backfill_run_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "social_tiktok_account_catalog_posts_last_backfill_run_id_idx" ON "social"."tiktok_account_catalog_posts" USING btree ("last_backfill_run_id");

-- social.tiktok_anomaly_events tiktok_anomaly_events_post_id_fkey -> social_tiktok_anomaly_events_post_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "social_tiktok_anomaly_events_post_id_idx" ON "social"."tiktok_anomaly_events" USING btree ("post_id");

-- social.tiktok_comments tiktok_comments_last_seen_run_id_fkey -> social_tiktok_comments_last_seen_run_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "social_tiktok_comments_last_seen_run_id_idx" ON "social"."tiktok_comments" USING btree ("last_seen_run_id");

-- social.tiktok_post_cast_members tiktok_post_cast_members_run_id_fkey -> social_tiktok_post_cast_members_run_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "social_tiktok_post_cast_members_run_id_idx" ON "social"."tiktok_post_cast_members" USING btree ("run_id");

-- social.twitter_account_catalog_posts twitter_account_catalog_posts_assigned_season_id_fkey -> social_twitter_account_catalog_posts_assigned_season_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "social_twitter_account_catalog_posts_assigned_season_id_idx" ON "social"."twitter_account_catalog_posts" USING btree ("assigned_season_id") WHERE assigned_season_id is not null;

-- social.twitter_account_catalog_posts twitter_account_catalog_posts_assigned_show_id_fkey -> social_twitter_account_catalog_posts_assigned_show_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "social_twitter_account_catalog_posts_assigned_show_id_idx" ON "social"."twitter_account_catalog_posts" USING btree ("assigned_show_id") WHERE assigned_show_id is not null;

-- social.twitter_account_catalog_posts twitter_account_catalog_posts_last_backfill_run_id_fkey -> social_twitter_account_catalog_posts_last_backfill_run_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "social_twitter_account_catalog_posts_last_backfill_run_id_idx" ON "social"."twitter_account_catalog_posts" USING btree ("last_backfill_run_id");

-- social.twitter_tweets twitter_tweets_last_seen_run_id_fkey -> social_twitter_tweets_last_seen_run_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "social_twitter_tweets_last_seen_run_id_idx" ON "social"."twitter_tweets" USING btree ("last_seen_run_id") WHERE last_seen_run_id is not null;

-- social.youtube_account_catalog_posts youtube_account_catalog_posts_assigned_season_id_fkey -> social_youtube_account_catalog_posts_assigned_season_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "social_youtube_account_catalog_posts_assigned_season_id_idx" ON "social"."youtube_account_catalog_posts" USING btree ("assigned_season_id") WHERE assigned_season_id is not null;

-- social.youtube_account_catalog_posts youtube_account_catalog_posts_assigned_show_id_fkey -> social_youtube_account_catalog_posts_assigned_show_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "social_youtube_account_catalog_posts_assigned_show_id_idx" ON "social"."youtube_account_catalog_posts" USING btree ("assigned_show_id") WHERE assigned_show_id is not null;

-- social.youtube_account_catalog_posts youtube_account_catalog_posts_last_backfill_run_id_fkey -> social_youtube_account_catalog_posts_last_backfill_run_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "social_youtube_account_catalog_posts_last_backfill_run_id_idx" ON "social"."youtube_account_catalog_posts" USING btree ("last_backfill_run_id");

-- social.youtube_channel_sync_state youtube_channel_sync_state_show_id_fkey -> social_youtube_channel_sync_state_show_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "social_youtube_channel_sync_state_show_id_idx" ON "social"."youtube_channel_sync_state" USING btree ("show_id");

-- social.youtube_comments youtube_comments_last_seen_run_id_fkey -> social_youtube_comments_last_seen_run_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "social_youtube_comments_last_seen_run_id_idx" ON "social"."youtube_comments" USING btree ("last_seen_run_id");

-- social.youtube_video_sync_state youtube_video_sync_state_show_id_fkey -> social_youtube_video_sync_state_show_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '30min';
CREATE INDEX CONCURRENTLY IF NOT EXISTS "social_youtube_video_sync_state_show_id_idx" ON "social"."youtube_video_sync_state" USING btree ("show_id");
