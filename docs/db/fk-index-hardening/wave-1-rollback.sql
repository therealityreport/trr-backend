-- wave-1 FK index hardening rollback
-- generated_at: 2026-04-17T19:09:04Z
-- apply with direct Postgres connectivity only

-- rollback admin.recent_people_views admin_recent_people_views_person_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "admin"."admin_recent_people_views_person_id_idx";

-- rollback social.account_hashtag_assignments social_account_hashtag_assignments_season_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "social"."social_account_hashtag_assignments_season_id_idx";

-- rollback social.account_hashtag_review_queue social_account_hashtag_review_queue_resolved_season_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "social"."social_account_hashtag_review_queue_resolved_season_id_idx";

-- rollback social.account_hashtag_review_queue social_account_hashtag_review_queue_resolved_show_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "social"."social_account_hashtag_review_queue_resolved_show_id_idx";

-- rollback social.dm_messages social_dm_messages_sender_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "social"."social_dm_messages_sender_id_idx";

-- rollback social.dm_read_receipts social_dm_read_receipts_last_read_message_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "social"."social_dm_read_receipts_last_read_message_id_idx";

-- rollback social.dm_read_receipts social_dm_read_receipts_user_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "social"."social_dm_read_receipts_user_id_idx";

-- rollback social.facebook_account_catalog_posts social_facebook_account_catalog_posts_assigned_season_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "social"."social_facebook_account_catalog_posts_assigned_season_id_idx";

-- rollback social.facebook_account_catalog_posts social_facebook_account_catalog_posts_assigned_show_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "social"."social_facebook_account_catalog_posts_assigned_show_id_idx";

-- rollback social.facebook_account_catalog_posts social_facebook_account_catalog_posts_last_backfill_run_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "social"."social_facebook_account_catalog_posts_last_backfill_run_id_idx";

-- rollback social.instagram_account_catalog_posts social_instagram_account_catalog_posts_assigned_season_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "social"."social_instagram_account_catalog_posts_assigned_season_id_idx";

-- rollback social.instagram_account_catalog_posts social_instagram_account_catalog_posts_assigned_show_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "social"."social_instagram_account_catalog_posts_assigned_show_id_idx";

-- rollback social.instagram_account_catalog_posts social_instagram_account_catalog_posts_last_backfill_run_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "social"."social_instagram_account_catalog_posts_last_backfill_run_id_idx";

-- rollback social.instagram_comments social_instagram_comments_last_seen_run_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "social"."social_instagram_comments_last_seen_run_id_idx";

-- rollback social.reactions social_reactions_user_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "social"."social_reactions_user_id_idx";

-- rollback social.reddit_period_post_matches social_reddit_period_post_matches_reddit_post_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "social"."social_reddit_period_post_matches_reddit_post_id_idx";

-- rollback social.reddit_period_post_matches social_reddit_period_post_matches_run_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "social"."social_reddit_period_post_matches_run_id_idx";

-- rollback social.reddit_period_post_matches social_reddit_period_post_matches_season_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "social"."social_reddit_period_post_matches_season_id_idx";

-- rollback social.reddit_refresh_runs social_reddit_refresh_runs_season_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "social"."social_reddit_refresh_runs_season_id_idx";

-- rollback social.scrape_jobs social_scrape_jobs_person_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "social"."social_scrape_jobs_person_id_idx";

-- rollback social.scrape_jobs social_scrape_jobs_show_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "social"."social_scrape_jobs_show_id_idx";

-- rollback social.scrape_runs social_scrape_runs_show_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "social"."social_scrape_runs_show_id_idx";

-- rollback social.scrape_workers social_scrape_workers_current_job_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "social"."social_scrape_workers_current_job_id_idx";

-- rollback social.scrape_workers social_scrape_workers_run_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "social"."social_scrape_workers_run_id_idx";

-- rollback social.shared_account_run_partitions social_shared_account_run_partitions_job_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "social"."social_shared_account_run_partitions_job_id_idx";

-- rollback social.shared_account_sources social_shared_account_sources_last_scrape_job_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "social"."social_shared_account_sources_last_scrape_job_id_idx";

-- rollback social.shared_account_sources social_shared_account_sources_last_scrape_run_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "social"."social_shared_account_sources_last_scrape_run_id_idx";

-- rollback social.shared_post_matches social_shared_post_matches_matched_show_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "social"."social_shared_post_matches_matched_show_id_idx";

-- rollback social.shared_post_matches social_shared_post_matches_run_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "social"."social_shared_post_matches_run_id_idx";

-- rollback social.shared_post_review_queue social_shared_post_review_queue_resolved_show_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "social"."social_shared_post_review_queue_resolved_show_id_idx";

-- rollback social.shared_post_review_queue social_shared_post_review_queue_run_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "social"."social_shared_post_review_queue_run_id_idx";

-- rollback social.sync_sessions social_sync_sessions_show_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "social"."social_sync_sessions_show_id_idx";

-- rollback social.threads_account_catalog_posts social_threads_account_catalog_posts_assigned_season_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "social"."social_threads_account_catalog_posts_assigned_season_id_idx";

-- rollback social.threads_account_catalog_posts social_threads_account_catalog_posts_assigned_show_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "social"."social_threads_account_catalog_posts_assigned_show_id_idx";

-- rollback social.threads_account_catalog_posts social_threads_account_catalog_posts_last_backfill_run_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "social"."social_threads_account_catalog_posts_last_backfill_run_id_idx";

-- rollback social.tiktok_account_catalog_posts social_tiktok_account_catalog_posts_assigned_season_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "social"."social_tiktok_account_catalog_posts_assigned_season_id_idx";

-- rollback social.tiktok_account_catalog_posts social_tiktok_account_catalog_posts_assigned_show_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "social"."social_tiktok_account_catalog_posts_assigned_show_id_idx";

-- rollback social.tiktok_account_catalog_posts social_tiktok_account_catalog_posts_last_backfill_run_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "social"."social_tiktok_account_catalog_posts_last_backfill_run_id_idx";

-- rollback social.tiktok_anomaly_events social_tiktok_anomaly_events_post_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "social"."social_tiktok_anomaly_events_post_id_idx";

-- rollback social.tiktok_comments social_tiktok_comments_last_seen_run_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "social"."social_tiktok_comments_last_seen_run_id_idx";

-- rollback social.tiktok_post_cast_members social_tiktok_post_cast_members_run_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "social"."social_tiktok_post_cast_members_run_id_idx";

-- rollback social.twitter_account_catalog_posts social_twitter_account_catalog_posts_assigned_season_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "social"."social_twitter_account_catalog_posts_assigned_season_id_idx";

-- rollback social.twitter_account_catalog_posts social_twitter_account_catalog_posts_assigned_show_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "social"."social_twitter_account_catalog_posts_assigned_show_id_idx";

-- rollback social.twitter_account_catalog_posts social_twitter_account_catalog_posts_last_backfill_run_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "social"."social_twitter_account_catalog_posts_last_backfill_run_id_idx";

-- rollback social.twitter_tweets social_twitter_tweets_last_seen_run_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "social"."social_twitter_tweets_last_seen_run_id_idx";

-- rollback social.youtube_account_catalog_posts social_youtube_account_catalog_posts_assigned_season_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "social"."social_youtube_account_catalog_posts_assigned_season_id_idx";

-- rollback social.youtube_account_catalog_posts social_youtube_account_catalog_posts_assigned_show_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "social"."social_youtube_account_catalog_posts_assigned_show_id_idx";

-- rollback social.youtube_account_catalog_posts social_youtube_account_catalog_posts_last_backfill_run_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "social"."social_youtube_account_catalog_posts_last_backfill_run_id_idx";

-- rollback social.youtube_channel_sync_state social_youtube_channel_sync_state_show_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "social"."social_youtube_channel_sync_state_show_id_idx";

-- rollback social.youtube_comments social_youtube_comments_last_seen_run_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "social"."social_youtube_comments_last_seen_run_id_idx";

-- rollback social.youtube_video_sync_state social_youtube_video_sync_state_show_id_idx
SET lock_timeout = '5s';
SET statement_timeout = '3h';
DROP INDEX CONCURRENTLY IF EXISTS "social"."social_youtube_video_sync_state_show_id_idx";
