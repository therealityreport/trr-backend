begin;

-- Enable RLS on live advisor-flagged tables in exposed schemas. Internal
-- direct DB lanes use privileged connections; this closes the PostgREST
-- exposure without changing app/backend write paths.
alter table admin.brand_families enable row level security;
alter table admin.brand_family_link_rules enable row level security;
alter table admin.brand_family_members enable row level security;
alter table admin.brand_family_wikipedia_show_links enable row level security;
alter table admin.brand_logo_assets enable row level security;
alter table admin.brand_logo_source_queries enable row level security;
alter table admin.brands_franchise_rules enable row level security;
alter table admin.cast_photo_people_tags enable row level security;
alter table admin.covered_shows enable row level security;
alter table admin.entity_logo_imports enable row level security;
alter table admin.network_streaming_completion enable row level security;
alter table admin.network_streaming_completion_attempts enable row level security;
alter table admin.network_streaming_discovery_state enable row level security;
alter table admin.network_streaming_logo_assets enable row level security;
alter table admin.network_streaming_overrides enable row level security;
alter table admin.network_streaming_sync_runs enable row level security;
alter table admin.person_cover_photos enable row level security;
alter table admin.person_reprocess_job_events enable row level security;
alter table admin.person_reprocess_jobs enable row level security;
alter table admin.recent_people_views enable row level security;
alter table admin.reddit_communities enable row level security;
alter table admin.reddit_discovery_posts enable row level security;
alter table admin.reddit_threads enable row level security;
alter table admin.season_cast_survey_roles enable row level security;
alter table admin.show_social_posts enable row level security;
alter table core.admin_operation_events enable row level security;
alter table core.admin_operations enable row level security;
alter table core.bravotv_image_runs enable row level security;
alter table core.cast_fandom enable row level security;
alter table core.cast_photos enable row level security;
alter table core.external_id_conflicts enable row level security;
alter table core.fandom_community_allowlist enable row level security;
alter table core.fandom_page_directory enable row level security;
alter table core.google_news_sync_jobs enable row level security;
alter table core.media_asset_variants enable row level security;
alter table core.media_assets enable row level security;
alter table core.media_links enable row level security;
alter table core.news_topic_taxonomy enable row level security;
alter table core.people_overrides enable row level security;
alter table core.season_fandom enable row level security;
alter table public.site_typography_assignments enable row level security;
alter table public.site_typography_sets enable row level security;
alter table public.survey_cast enable row level security;
alter table public.survey_episodes enable row level security;
alter table public.survey_global_profile_responses enable row level security;
alter table public.survey_rhop_s10_responses enable row level security;
alter table public.survey_rhoslc_s6_responses enable row level security;
alter table public.survey_show_palette_library enable row level security;
alter table public.survey_show_seasons enable row level security;
alter table public.survey_shows enable row level security;
alter table public.survey_x_responses enable row level security;
alter table public.surveys enable row level security;

-- Views in exposed schemas should execute with caller permissions.
alter view core.episode_appearances set (security_invoker = on);
alter view core.imdb_series set (security_invoker = on);
alter view core.show_cast set (security_invoker = on);
alter view core.tmdb_series set (security_invoker = on);
alter view core.v_cast_photos set (security_invoker = on);
alter view core.v_cast_summary set (security_invoker = on);
alter view core.v_episode_appearances set (security_invoker = on);
alter view core.v_episode_appearances_from_credits set (security_invoker = on);
alter view core.v_episode_cast set (security_invoker = on);
alter view core.v_episode_credits set (security_invoker = on);
alter view core.v_episode_images_served_media_v2 set (security_invoker = on);
alter view core.v_media_ingest_summary set (security_invoker = on);
alter view core.v_person_images set (security_invoker = on);
alter view core.v_person_images_served_media set (security_invoker = on);
alter view core.v_person_images_served_media_v2 set (security_invoker = on);
alter view core.v_person_show_seasons set (security_invoker = on);
alter view core.v_season_cast set (security_invoker = on);
alter view core.v_season_images_served_media_v2 set (security_invoker = on);
alter view core.v_show_cast set (security_invoker = on);
alter view core.v_show_cast_from_credits set (security_invoker = on);
alter view core.v_show_cast_roles_enriched set (security_invoker = on);
alter view core.v_show_images set (security_invoker = on);
alter view core.v_show_images_served set (security_invoker = on);
alter view core.v_show_images_served_media set (security_invoker = on);
alter view core.v_show_images_served_media_v2 set (security_invoker = on);
alter view core.v_show_seasons set (security_invoker = on);
alter view social.v_tiktok_daily_analytics set (security_invoker = on);
alter view social.v_tiktok_weekly_analytics set (security_invoker = on);

-- Lock security definer functions to an explicit search_path.
alter function core.merge_shows(uuid, uuid) set search_path = core, public;
alter function core.set_primary_media_link(text, uuid, text, uuid) set search_path = core, public;
alter function core.upsert_cast_photos_by_canonical(jsonb) set search_path = core, public;
alter function core.upsert_cast_photos_by_identity(jsonb) set search_path = core, public;
alter function core.upsert_person_images(jsonb) set search_path = core, public;
alter function core.upsert_show_images_by_identity(jsonb) set search_path = core, public;
alter function core.upsert_tmdb_show_images_by_identity(jsonb) set search_path = core, public;

commit;
