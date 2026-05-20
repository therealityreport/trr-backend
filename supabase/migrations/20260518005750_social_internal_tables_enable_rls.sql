alter table social.account_hashtag_assignments enable row level security;
alter table social.avatar_registry enable row level security;
alter table social.instagram_profile_pagination_state enable row level security;
alter table social.sync_sessions enable row level security;
alter table social.twitter_interaction_fetch_state enable row level security;
alter table social.twitter_scrape_queries enable row level security;
alter table social.twitter_scrape_query_tweets enable row level security;

-- These tables store internal scrape state. Keep API roles denied explicitly
-- while allowing privileged backend/service-role access to continue.
drop policy if exists deny_api_access_account_hashtag_assignments
  on social.account_hashtag_assignments;
create policy deny_api_access_account_hashtag_assignments
  on social.account_hashtag_assignments
  as restrictive
  for all
  to public
  using (false)
  with check (false);

drop policy if exists deny_api_access_avatar_registry
  on social.avatar_registry;
create policy deny_api_access_avatar_registry
  on social.avatar_registry
  as restrictive
  for all
  to public
  using (false)
  with check (false);

drop policy if exists deny_api_access_instagram_profile_pagination_state
  on social.instagram_profile_pagination_state;
create policy deny_api_access_instagram_profile_pagination_state
  on social.instagram_profile_pagination_state
  as restrictive
  for all
  to public
  using (false)
  with check (false);

drop policy if exists deny_api_access_sync_sessions
  on social.sync_sessions;
create policy deny_api_access_sync_sessions
  on social.sync_sessions
  as restrictive
  for all
  to public
  using (false)
  with check (false);

drop policy if exists deny_api_access_twitter_interaction_fetch_state
  on social.twitter_interaction_fetch_state;
create policy deny_api_access_twitter_interaction_fetch_state
  on social.twitter_interaction_fetch_state
  as restrictive
  for all
  to public
  using (false)
  with check (false);

drop policy if exists deny_api_access_twitter_scrape_queries
  on social.twitter_scrape_queries;
create policy deny_api_access_twitter_scrape_queries
  on social.twitter_scrape_queries
  as restrictive
  for all
  to public
  using (false)
  with check (false);

drop policy if exists deny_api_access_twitter_scrape_query_tweets
  on social.twitter_scrape_query_tweets;
create policy deny_api_access_twitter_scrape_query_tweets
  on social.twitter_scrape_query_tweets
  as restrictive
  for all
  to public
  using (false)
  with check (false);

drop policy if exists deny_api_access_instagram_following_snapshots
  on social.instagram_profile_following_snapshots;
create policy deny_api_access_instagram_following_snapshots
  on social.instagram_profile_following_snapshots
  as restrictive
  for all
  to public
  using (false)
  with check (false);

drop policy if exists deny_api_access_instagram_relationship_snapshot_items
  on social.instagram_profile_relationship_snapshot_items;
create policy deny_api_access_instagram_relationship_snapshot_items
  on social.instagram_profile_relationship_snapshot_items
  as restrictive
  for all
  to public
  using (false)
  with check (false);
