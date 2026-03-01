# Repository Structure
```markdown
trr-backend
├── .claude
│   ├── commands
│   │   ├── trr-impl.md
│   │   ├── trr-plan.md
│   │   ├── trr-pr.md
│   │   ├── trr-spec.md
│   │   ├── trr-validate.md
│   │   └── trr-wt-new.md
│   ├── hooks
│   │   ├── before-bash.md
│   │   └── on-stop.md
│   └── plans
│       └── v2-runs-implementation.md
├── .config
│   └── wt.toml
├── .dockerignore
├── .env.example
├── .github
│   └── workflows
│       ├── ci.yml
│       ├── mirror-media-assets.yml
│       ├── repo_map.yml
│       └── secret-scan.yml
├── .gitignore
├── .gitleaks.toml
├── .python-version
├── AGENTS.md
├── BRANCHING_STRATEGY.md
├── CLAUDE.md
├── CONTRIBUTING.md
├── Dockerfile
├── Makefile
├── README.md
├── REPO_STRUCTURE.md
├── api
│   ├── __init__.py
│   ├── auth.py
│   ├── deps.py
│   ├── main.py
│   ├── realtime
│   │   ├── __init__.py
│   │   ├── broker.py
│   │   └── events.py
│   ├── routers
│   │   ├── __init__.py
│   │   ├── admin_asset_batch_jobs.py
│   │   ├── admin_asset_flags.py
│   │   ├── admin_brands.py
│   │   ├── admin_cast.py
│   │   ├── admin_cast_photos.py
│   │   ├── admin_fandom_sync.py
│   │   ├── admin_image_counts.py
│   │   ├── admin_media_assets.py
│   │   ├── admin_person_images.py
│   │   ├── admin_scrape.py
│   │   ├── admin_show_bravo.py
│   │   ├── admin_show_icons.py
│   │   ├── admin_show_links.py
│   │   ├── admin_show_news.py
│   │   ├── admin_show_roles.py
│   │   ├── admin_show_sync.py
│   │   ├── discussions.py
│   │   ├── dms.py
│   │   ├── screenalytics.py
│   │   ├── screenalytics_runs_v2.py
│   │   ├── shows.py
│   │   ├── socials.py
│   │   ├── surveys.py
│   │   └── ws.py
│   └── screenalytics_auth.py
├── backfill_tmdb_show_details.py
├── docs
│   ├── HISTORY_PURGE.md
│   ├── README_local.md
│   ├── Repository
│   │   ├── README.md
│   │   ├── diagrams
│   │   │   ├── git_workflow.md
│   │   │   └── system_maps.md
│   │   └── generated
│   │       ├── .gitkeep
│   │       ├── CODE_IMPORT_GRAPH.md
│   │       ├── REPO_STRUCTURE.md
│   │       ├── REPO_STRUCTURE.mermaid.md
│   │       ├── SCRIPTS_FLOW.md
│   │       └── rendered
│   │           ├── CODE_IMPORT_GRAPH-1.svg
│   │           ├── REPO_STRUCTURE.mermaid-1.svg
│   │           ├── SCRIPTS_FLOW-1.svg
│   │           ├── git_workflow-1.svg
│   │           ├── system_maps-1.svg
│   │           └── system_maps-2.svg
│   ├── SECURITY.md
│   ├── ai
│   │   └── HANDOFF.md
│   ├── api
│   │   └── run.md
│   ├── architecture
│   │   ├── imdb_fullcredits_resilience_implementation_plan.md
│   │   ├── imdb_fullcredits_resilience_spec.md
│   │   ├── imdb_graphql_migration_spec.md
│   │   ├── integrations.md
│   │   └── pipeline.md
│   ├── architecture.md
│   ├── cloud
│   │   ├── cloud_setup.md
│   │   ├── quick_cloud_setup.md
│   │   └── setup_codespaces_credentials.md
│   ├── cross-collab
│   │   ├── README.md
│   │   ├── TASK1
│   │   │   ├── OTHER_PROJECTS.md
│   │   │   └── PLAN.md
│   │   ├── TASK10
│   │   │   ├── OTHER_PROJECTS.md
│   │   │   ├── PLAN.md
│   │   │   └── STATUS.md
│   │   ├── TASK2
│   │   │   ├── OTHER_PROJECTS.md
│   │   │   └── PLAN.md
│   │   ├── TASK3
│   │   │   ├── OTHER_PROJECTS.md
│   │   │   └── PLAN.md
│   │   ├── TASK4
│   │   │   ├── OTHER_PROJECTS.md
│   │   │   ├── PLAN.md
│   │   │   └── STATUS.md
│   │   ├── TASK5
│   │   │   ├── OTHER_PROJECTS.md
│   │   │   ├── PLAN.md
│   │   │   └── STATUS.md
│   │   ├── TASK6
│   │   │   ├── OTHER_PROJECTS.md
│   │   │   ├── PLAN.md
│   │   │   └── STATUS.md
│   │   ├── TASK7
│   │   │   ├── OTHER_PROJECTS.md
│   │   │   ├── PLAN.md
│   │   │   └── STATUS.md
│   │   ├── TASK8
│   │   │   ├── OTHER_PROJECTS.md
│   │   │   ├── PLAN.md
│   │   │   └── STATUS.md
│   │   └── TASK9
│   │       ├── OTHER_PROJECTS.md
│   │       ├── PLAN.md
│   │       └── STATUS.md
│   ├── db
│   │   ├── commands.md
│   │   ├── schema.md
│   │   └── verification.md
│   ├── deploy
│   │   └── cloud_run.md
│   ├── images
│   │   └── debug_imdb_credits.png
│   ├── legacy
│   │   ├── README.md
│   │   ├── README_local_google_sheets.md
│   │   ├── SHEET_EDIT_MAPPING.md
│   │   ├── architecture_google_sheets.md
│   │   ├── cloud_quick_setup_google_sheets.md
│   │   ├── cloud_setup_google_sheets.md
│   │   ├── codespaces_google_credentials.md
│   │   └── google_sheets_pipeline.md
│   ├── plans
│   │   ├── 2026-01-28-surveys-supabase-auth.md
│   │   ├── 2026-02-12-show-icons.md
│   │   └── repo_cleanup.md
│   ├── runbooks
│   │   ├── credits_v2_rollout.md
│   │   ├── postgrest_schema_cache.md
│   │   ├── rhoslc-show-admin-backfill.md
│   │   ├── show_import_job.md
│   │   ├── social_worker_queue_ops.md
│   │   └── supabase_migration_history_repair.md
│   └── workflows
│       └── VIBE_CODING.md
├── pytest.ini
├── requirements.in
├── requirements.lock.txt
├── requirements.txt
├── resolve_tmdb_ids_via_find.py
├── ruff.toml
├── scripts
│   ├── README.md
│   ├── _sync_common.py
│   ├── backfill
│   │   ├── backfill_bravo_video_thumbnails.py
│   │   ├── backfill_credits.py
│   │   ├── backfill_media_assets.py
│   │   └── backfill_tmdb_show_details.py
│   ├── backfill_bravo_video_thumbnails.py
│   ├── backfill_credits.py
│   ├── backfill_media_asset_variants.py
│   ├── backfill_media_assets.py
│   ├── backfill_tmdb_show_details.py
│   ├── check_env_example.py
│   ├── cleanup
│   │   └── cleanup_fandom_mismatches.py
│   ├── cleanup_expired_media_uploads.py
│   ├── db
│   │   ├── README.md
│   │   ├── guard_core_schema.sql
│   │   ├── reload_postgrest_schema.sql
│   │   ├── run_sql.sh
│   │   ├── verify_media_unification.sql
│   │   └── verify_pre_0033_cleanup.sql
│   ├── dev
│   │   └── doctor.py
│   ├── download_scraped_images_local.py
│   ├── enrich
│   │   ├── enrich_show_cast.py
│   │   ├── imdb_show_enrichment.py
│   │   └── rhoslc_fandom_enrichment.py
│   ├── enrich_show_cast.py
│   ├── fix_repo_structure_mermaid.py
│   ├── generate_repo_mermaid.py
│   ├── imdb_show_enrichment.py
│   ├── import
│   │   ├── download_scraped_images_local.py
│   │   ├── import_fandom_gallery_photos.py
│   │   ├── import_imdb_cast_episode_appearances.py
│   │   ├── import_shows_from_lists.py
│   │   └── run_show_import_job.py
│   ├── import_fandom_gallery_photos.py
│   ├── import_imdb_cast_episode_appearances.py
│   ├── import_shows_from_lists.py
│   ├── legacy
│   │   └── test_connection.py
│   ├── media
│   │   ├── README.md
│   │   ├── backfill_media_asset_variants.py
│   │   ├── cleanup_expired_media_uploads.py
│   │   ├── mirror_cast_photos_to_s3.py
│   │   ├── mirror_media_assets_to_s3.py
│   │   ├── mirror_show_images_to_s3.py
│   │   ├── rebuild_hosted_urls.py
│   │   ├── repair_cast_photo_hosts.py
│   │   ├── repair_gallery_hosts.py
│   │   ├── restore_changed_originals.py
│   │   └── restore_person_gallery_base_previews.py
│   ├── mirror_cast_photos_to_s3.py
│   ├── mirror_media_assets_to_s3.py
│   ├── mirror_show_images_to_s3.py
│   ├── rebuild_hosted_urls.py
│   ├── reload_postgrest_schema.sh
│   ├── reload_postgrest_schema.sql
│   ├── resolve_tmdb_ids_via_find.py
│   ├── rhoslc_fandom_enrichment.py
│   ├── run_show_import_job.py
│   ├── shows
│   │   ├── backfill_bravo_person_source_links.py
│   │   └── cleanup_invalid_person_knowledge_links.py
│   ├── socials
│   │   ├── __init__.py
│   │   ├── backfill_bravo_missing_platform_targets.py
│   │   ├── backfill_instagram_metadata_and_media.py
│   │   ├── backfill_social_media_mirror_jobs.py
│   │   ├── backfill_social_post_tokens.py
│   │   ├── backfill_tiktok_saves.py
│   │   ├── cleanup_youtube_false_positives.py
│   │   ├── instagram
│   │   │   ├── __init__.py
│   │   │   ├── instagram_cookies.example.json
│   │   │   └── scrape.py
│   │   ├── repair_social_hosted_urls.py
│   │   ├── start_worker_pool.sh
│   │   ├── tiktok
│   │   │   ├── __init__.py
│   │   │   └── scrape.py
│   │   ├── twitter
│   │   │   ├── __init__.py
│   │   │   └── scrape.py
│   │   ├── worker.py
│   │   └── youtube
│   │       ├── __init__.py
│   │       └── scrape.py
│   ├── supabase
│   │   └── generate_schema_docs.py
│   ├── sync
│   │   ├── resolve_tmdb_ids_via_find.py
│   │   ├── sync_all_tables.py
│   │   ├── sync_cast_batch.py
│   │   ├── sync_cast_photos.py
│   │   ├── sync_episode_appearances.py
│   │   ├── sync_episodes.py
│   │   ├── sync_networks_streaming_links.py
│   │   ├── sync_people.py
│   │   ├── sync_season_episode_images.py
│   │   ├── sync_seasons.py
│   │   ├── sync_seasons_episodes.py
│   │   ├── sync_show_batch.py
│   │   ├── sync_show_cast.py
│   │   ├── sync_show_complete.py
│   │   ├── sync_show_images.py
│   │   ├── sync_show_logos.py
│   │   ├── sync_shows.py
│   │   ├── sync_shows_all.py
│   │   ├── sync_tmdb_person_images.py
│   │   ├── sync_tmdb_show_entities.py
│   │   └── sync_tmdb_watch_providers.py
│   ├── sync_all_tables.py
│   ├── sync_cast_batch.py
│   ├── sync_cast_photos.py
│   ├── sync_episode_appearances.py
│   ├── sync_episodes.py
│   ├── sync_networks_streaming_links.py
│   ├── sync_people.py
│   ├── sync_season_episode_images.py
│   ├── sync_seasons.py
│   ├── sync_seasons_episodes.py
│   ├── sync_show_batch.py
│   ├── sync_show_cast.py
│   ├── sync_show_complete.py
│   ├── sync_show_images.py
│   ├── sync_show_logos.py
│   ├── sync_shows.py
│   ├── sync_shows_all.py
│   ├── sync_tmdb_person_images.py
│   ├── sync_tmdb_show_entities.py
│   ├── sync_tmdb_watch_providers.py
│   ├── validate_supabase_timeouts.py
│   ├── verify
│   │   ├── validate_supabase_timeouts.py
│   │   ├── verify_credits_parity.py
│   │   ├── verify_media_unification.py
│   │   └── verify_schema.py
│   ├── verify_credits_parity.py
│   ├── verify_media_unification.py
│   └── verify_schema.py
├── skills
│   └── database-designer
│       ├── SKILL.md
│       └── references
│           ├── examples.md
│           ├── playbooks.md
│           ├── repo-context.md
│           ├── templates.sql
│           └── tooling.md
├── start-api.sh
├── supabase
│   ├── .gitignore
│   ├── config.toml
│   ├── migrations
│   │   ├── 0001_init.sql
│   │   ├── 0002_social.sql
│   │   ├── 0003_dms.sql
│   │   ├── 0004_core_shows.sql
│   │   ├── 0005_show_images.sql
│   │   ├── 0006_show_images_grants.sql
│   │   ├── 0007_core_shows_tmdb_id.sql
│   │   ├── 0008_show_images_tmdb_id.sql
│   │   ├── 0009_show_images_view.sql
│   │   ├── 0010_show_images_no_votes.sql
│   │   ├── 0011_show_images_view_no_votes.sql
│   │   ├── 0012_seasons_and_episodes.sql
│   │   ├── 0013_season_images.sql
│   │   ├── 0014_show_seasons_view.sql
│   │   ├── 0015_seasons_show_name.sql
│   │   ├── 0016_seasons_episode_id_arrays.sql
│   │   ├── 0017_episodes_show_name.sql
│   │   ├── 0018_imdb_cast_episode_appearances.sql
│   │   ├── 0019_imdb_cast_grants.sql
│   │   ├── 0020_reorder_show_tables.sql
│   │   ├── 0021_reorder_people_cast_seasons_episodes.sql
│   │   ├── 0022_episode_appearances_export_view.sql
│   │   ├── 0023_episode_appearances_export_view_total_episodes.sql
│   │   ├── 0024_episode_appearances_aggregate.sql
│   │   ├── 0025_sync_state.sql
│   │   ├── 0026_add_imdb_meta_to_core_shows.sql
│   │   ├── 0027_show_images_media_sources.sql
│   │   ├── 0028_normalize_shows_add_columns.sql
│   │   ├── 0029_create_source_tables.sql
│   │   ├── 0030_create_normalized_child_tables.sql
│   │   ├── 0031_update_show_images_typed.sql
│   │   ├── 0032_backfill_normalized_data.sql
│   │   ├── 0033_cleanup_legacy_jsonb_columns.sql
│   │   ├── 0034_show_images_constraints_and_show_flags.sql
│   │   ├── 0035_show_images_upsert_rpc.sql
│   │   ├── 0036_show_merge_helpers.sql
│   │   ├── 0037_collapse_show_attributes.sql
│   │   ├── 0038_update_merge_shows_arrays.sql
│   │   ├── 0039_drop_child_tables.sql
│   │   ├── 0040_create_cast_photos.sql
│   │   ├── 0041_create_cast_fandom_and_extend_cast_photos.sql
│   │   ├── 0042_revoke_cast_public_access.sql
│   │   ├── 0043_cast_photos_add_hosted_fields.sql
│   │   ├── 0044_create_cast_tmdb.sql
│   │   ├── 0045_show_images_add_hosted_fields.sql
│   │   ├── 0046_cast_photos_allow_tmdb_source.sql
│   │   ├── 0047_add_show_source_metadata.sql
│   │   ├── 0048_create_tmdb_entities_and_watch_providers.sql
│   │   ├── 0049_rename_tmdb_dimension_tables.sql
│   │   ├── 0050_drop_or_view_tmdb_imdb_series.sql
│   │   ├── 0051_season_images_add_hosted_fields.sql
│   │   ├── 0052_season_images_add_metadata_fields.sql
│   │   ├── 0053_add_show_cast_source_tracking.sql
│   │   ├── 0054_show_images_upsert_rpc_remove_votes.sql
│   │   ├── 0055_expand_show_cast_source_types_graphql.sql
│   │   ├── 0056_create_person_images.sql
│   │   ├── 0057_add_alternative_names_to_shows.sql
│   │   ├── 0058_create_media_assets.sql
│   │   ├── 0059_create_media_links.sql
│   │   ├── 0060_create_media_served_views.sql
│   │   ├── 0061_add_media_assets_ingest_fields.sql
│   │   ├── 0062_create_v_media_ingest_summary.sql
│   │   ├── 0063_set_primary_media_link_rpc.sql
│   │   ├── 0064_create_media_uploads.sql
│   │   ├── 0065_create_credits_tables.sql
│   │   ├── 0066_create_credits_validation_views.sql
│   │   ├── 0067_create_episode_images.sql
│   │   ├── 0068_prep_helpers.sql
│   │   ├── 0069_sources.sql
│   │   ├── 0070_external_ids.sql
│   │   ├── 0071_source_snapshots.sql
│   │   ├── 0072_media_constraints.sql
│   │   ├── 0073_backfill_external_ids.sql
│   │   ├── 0074_backfill_source_snapshots.sql
│   │   ├── 0075_backfill_media_links.sql
│   │   ├── 0076_primary_assignment.sql
│   │   ├── 0077_validation_gates.sql
│   │   ├── 0078_compat_views.sql
│   │   ├── 0079_deprecations.sql
│   │   ├── 0080_bridge_legacy_media_links.sql
│   │   ├── 0081_bridge_show_source_snapshots.sql
│   │   ├── 0082_create_show_alternative_names.sql
│   │   ├── 0083_grant_show_source_history_sequences.sql
│   │   ├── 0084_grant_media_assets_links.sql
│   │   ├── 0085_grant_media_uploads.sql
│   │   ├── 0086_create_pipeline_schema.sql
│   │   ├── 0087_screenalytics_cast_views.sql
│   │   ├── 0088_person_images_view.sql
│   │   ├── 0089_survey_response_unique_per_user.sql
│   │   ├── 0090_survey_submit_response_rpc.sql
│   │   ├── 0092_survey_slug_column.sql
│   │   ├── 0093_create_screenalytics_v2_runs.sql
│   │   ├── 0094_fix_bridge_cast_photos_updates.sql
│   │   ├── 0095_cast_overrides.sql
│   │   ├── 0096_image_archive_columns.sql
│   │   ├── 0097_image_audit_log.sql
│   │   ├── 0098_fix_bridge_hosted_sha256_conflict.sql
│   │   ├── 0099_admin_cast_photo_people_tags.sql
│   │   ├── 0100_facebank_seed_media_links.sql
│   │   ├── 0101_social_scrape_tables.sql
│   │   ├── 0102_screenalytics_face_bank_images.sql
│   │   ├── 0103_screenalytics_video_asset_cast_candidates.sql
│   │   ├── 0104_screenalytics_v1_operational_tables.sql
│   │   ├── 0105_screenalytics_outbox_events.sql
│   │   ├── 0106_drop_games_schema.sql
│   │   ├── 0107_drop_legacy_cast_tables.sql
│   │   ├── 0108_modify_core_shows_consolidate_columns.sql
│   │   ├── 0109_enrich_core_people_multisource.sql
│   │   ├── 0110_enrich_core_credit_occurrences.sql
│   │   ├── 0111_add_social_columns_dimension_tables.sql
│   │   ├── 0112_expand_people_overrides_handles.sql
│   │   ├── 0113_extend_social_scrape_jobs_platforms.sql
│   │   ├── 0114_create_core_v_cast_summary.sql
│   │   ├── 0115_reconcile_screenalytics_v2_tables.sql
│   │   ├── 0116_archive_media_assets_and_show_images.sql
│   │   ├── 0117_add_bravo_source.sql
│   │   ├── 0118_social_season_analytics.sql
│   │   ├── 0119_create_media_asset_variants.sql
│   │   ├── 0120_show_admin_links_and_roles.sql
│   │   ├── 0121_social_scrape_runs.sql
│   │   ├── 0122_social_scrape_jobs_queue_fields.sql
│   │   ├── 0123_social_scrape_jobs_queue_indexes.sql
│   │   ├── 0124_social_thumbnails_and_reddit_sources.sql
│   │   ├── 0125_social_analytics_query_indexes.sql
│   │   ├── 0126_social_comment_lifecycle_flags.sql
│   │   ├── 0127_add_network_provider_link_fields.sql
│   │   ├── 0128_add_network_provider_monochrome_logo_fields.sql
│   │   ├── 0129_add_google_news_source.sql
│   │   ├── 0130_social_worker_heartbeat_and_comment_id_guardrails.sql
│   │   ├── 0131_network_streaming_completion_and_overrides.sql
│   │   ├── 0132_instagram_permalink_metadata_and_media_mirroring.sql
│   │   ├── 0133_fandom_sync_expansion.sql
│   │   ├── 0134_optimize_cast_role_members.sql
│   │   ├── 0135_network_streaming_logo_assets.sql
│   │   ├── 0136_production_logo_parity_and_imports.sql
│   │   ├── 0137_instagram_media_mirror_diagnostics.sql
│   │   ├── 0138_news_feature_hardening.sql
│   │   ├── 0139_add_fandom_allowlist_table.sql
│   │   ├── 0140_google_news_sync_job_heartbeat.sql
│   │   ├── 0141_social_analytics_hot_path_indexes.sql
│   │   ├── 0143_network_streaming_discovery_state.sql
│   │   ├── 0144_network_streaming_sync_runs.sql
│   │   ├── 0145_cross_platform_media_mirror_fields_and_job_types.sql
│   │   ├── 0146_entity_links_unique_per_show.sql
│   │   ├── 0147_instagram_enhanced_metadata.sql
│   │   ├── 0148_youtube_shorts_flags.sql
│   │   ├── 0149_tiktok_comment_media_and_metadata.sql
│   │   ├── 0150_add_show_icons_table.sql
│   │   ├── 0151_twitter_user_metadata_fields.sql
│   │   ├── 0152_add_facebook_and_meta_threads_social_platforms.sql
│   │   ├── 0153_tiktok_saves_count.sql
│   │   ├── 0154_add_show_slug.sql
│   │   ├── 0155_social_token_columns_for_cross_platform_posts.sql
│   │   ├── 0156_add_source_asset_id_to_cast_photos.sql
│   │   ├── 0157_reddit_refresh_pipeline.sql
│   │   └── 0158_reddit_period_match_flair_mode.sql
│   └── seed.sql
├── test_connection.py
├── tests
│   ├── __init__.py
│   ├── api
│   │   ├── routers
│   │   │   ├── __init__.py
│   │   │   ├── test_admin_asset_batch_jobs.py
│   │   │   ├── test_admin_asset_flags.py
│   │   │   ├── test_admin_brands.py
│   │   │   ├── test_admin_cast_photos.py
│   │   │   ├── test_admin_fandom_sync.py
│   │   │   ├── test_admin_image_counts_fallback.py
│   │   │   ├── test_admin_person_images.py
│   │   │   ├── test_admin_person_images_auto_count_enrichment.py
│   │   │   ├── test_admin_scrape_contracts.py
│   │   │   ├── test_admin_show_bravo.py
│   │   │   ├── test_admin_show_icons.py
│   │   │   ├── test_admin_show_links.py
│   │   │   ├── test_admin_show_news.py
│   │   │   ├── test_admin_show_roles.py
│   │   │   ├── test_admin_show_sync.py
│   │   │   ├── test_admin_show_sync_imdb_mediaindex_context.py
│   │   │   ├── test_socials_reddit_refresh_routes.py
│   │   │   ├── test_socials_season_analytics.py
│   │   │   └── test_socials_twitter_admin_routes.py
│   │   ├── test_auth.py
│   │   ├── test_screenalytics_ingest_endpoints.py
│   │   ├── test_screenalytics_runs_v2.py
│   │   └── test_survey_submit.py
│   ├── db
│   │   ├── __init__.py
│   │   ├── test_connection_resolution.py
│   │   ├── test_pg_pool.py
│   │   ├── test_supabase_timeout.py
│   │   └── test_survey_submit_rpc.sql
│   ├── fixtures
│   │   ├── fandom
│   │   │   ├── andy_cohen_gallery_sample.html
│   │   │   ├── lisa_barlow_infobox.html
│   │   │   ├── lisa_barlow_person_live_infobox_sample.html
│   │   │   ├── lisa_barlow_person_sample.html
│   │   │   └── rhoslc_cast_table_sample.html
│   │   ├── imdb
│   │   │   ├── episodes_page_overview_one_season_sample.html
│   │   │   ├── episodes_page_overview_sample.html
│   │   │   ├── episodes_page_season1_next_data_sample.html
│   │   │   ├── episodes_page_season3_sample.html
│   │   │   ├── fullcredits_cast_sample.html
│   │   │   ├── list_html_fallback_sample.html
│   │   │   ├── list_jsonld_sample.html
│   │   │   ├── list_sample.html
│   │   │   ├── list_sample_page2.html
│   │   │   ├── mediaindex_tt8819906_sample.html
│   │   │   ├── mediaindex_viewer_graphql_tt8819906_sample.html
│   │   │   ├── person_mediaindex_nm11883948_sample.html
│   │   │   ├── person_mediaviewer_nm11883948_rm1679992066_sample.html
│   │   │   ├── section_images_sample.html
│   │   │   ├── title_list_main_page_sample.json
│   │   │   ├── title_page_sample.html
│   │   │   └── title_page_tt8819906_sample.html
│   │   ├── scraping
│   │   │   └── eonline_pinterest_sample.html
│   │   ├── socials
│   │   │   └── recon
│   │   │       └── facebook_threads_recon_fixture_pack.json
│   │   ├── tmdb
│   │   │   ├── find_by_imdb_id_sample.json
│   │   │   ├── tv_alternative_titles_sample.json
│   │   │   ├── tv_details_full_sample.json
│   │   │   ├── tv_details_sample.json
│   │   │   ├── tv_images_sample.json
│   │   │   ├── tv_season_details_sample.json
│   │   │   └── tv_watch_providers_sample.json
│   │   └── wikipedia
│   │       └── rhoslc_cast_table_sample.html
│   ├── ingestion
│   │   ├── test_cast_photo_sources_fandom.py
│   │   ├── test_cast_photo_sources_imdb.py
│   │   ├── test_episode_appearances_upsert.py
│   │   ├── test_fandom_person_scraper.py
│   │   ├── test_imdb_show_mediaindex_rows.py
│   │   ├── test_show_cast_matrix_scraper.py
│   │   ├── test_show_importer_episode_precedence.py
│   │   ├── test_show_importer_metadata_enrichment.py
│   │   ├── test_show_importer_tmdb_details_links_imdb_show.py
│   │   ├── test_show_metadata_enricher.py
│   │   └── test_tmdb_show_backfill.py
│   ├── integrations
│   │   ├── fandom
│   │   │   ├── test_fandom_discovery.py
│   │   │   ├── test_fandom_infobox_parser.py
│   │   │   └── test_fandom_search.py
│   │   ├── imdb
│   │   │   ├── test_episodic_client_normalization.py
│   │   │   ├── test_fullcredits_cast_parser.py
│   │   │   ├── test_graphql_client.py
│   │   │   ├── test_graphql_fallback_integration.py
│   │   │   ├── test_graphql_operations.py
│   │   │   ├── test_imdb_episodes_persistence.py
│   │   │   ├── test_imdb_images.py
│   │   │   ├── test_imdb_list_graphql_client_parsing.py
│   │   │   ├── test_mediaindex_images.py
│   │   │   ├── test_person_gallery_parser.py
│   │   │   ├── test_person_image_extraction.py
│   │   │   └── test_title_page_metadata.py
│   │   ├── test_brandfetch.py
│   │   ├── test_logopedia.py
│   │   └── tmdb
│   │       ├── test_tmdb_person.py
│   │       ├── test_tmdb_season_enrichment.py
│   │       ├── test_tmdb_tv_details_persistence.py
│   │       └── test_tmdb_tv_images_persistence.py
│   ├── media
│   │   ├── __init__.py
│   │   ├── test_s3_mirror.py
│   │   ├── test_s3_mirror_icons.py
│   │   └── test_user_uploads.py
│   ├── migrations
│   │   └── test_show_source_metadata_migrations.py
│   ├── pipeline
│   │   ├── __init__.py
│   │   ├── test_models.py
│   │   ├── test_orchestrator.py
│   │   └── test_stages.py
│   ├── repositories
│   │   ├── test_cast_photos_upsert.py
│   │   ├── test_cast_tmdb.py
│   │   ├── test_credits.py
│   │   ├── test_credits_integration.py
│   │   ├── test_media_assets_mirroring.py
│   │   ├── test_media_assets_transform.py
│   │   ├── test_pgrst204_retry.py
│   │   ├── test_reddit_refresh.py
│   │   ├── test_show_images_dual_write.py
│   │   ├── test_shows_preflight.py
│   │   └── test_social_season_analytics.py
│   ├── scraping
│   │   ├── test_bravo_parser.py
│   │   ├── test_google_news_parser.py
│   │   └── test_url_image_scraper.py
│   ├── scripts
│   │   ├── test_backfill_bravo_person_source_links.py
│   │   ├── test_backfill_instagram_metadata_and_media.py
│   │   ├── test_backfill_social_media_mirror_jobs.py
│   │   ├── test_download_scraped_images_local.py
│   │   ├── test_import_shows_from_lists_merge.py
│   │   ├── test_import_shows_from_lists_parsing.py
│   │   ├── test_import_shows_from_lists_upsert.py
│   │   ├── test_repair_gallery_hosts.py
│   │   ├── test_repair_social_hosted_urls.py
│   │   ├── test_restore_changed_originals.py
│   │   ├── test_social_worker.py
│   │   ├── test_sync_episode_appearances_season_coverage.py
│   │   ├── test_sync_incremental.py
│   │   ├── test_sync_networks_streaming_links.py
│   │   ├── test_sync_tmdb_watch_providers.py
│   │   ├── test_twitter_scrape_cli.py
│   │   └── test_verify_credits_parity.py
│   ├── socials
│   │   ├── test_comment_scraper_fixes.py
│   │   ├── test_crawlee_auth_preflight.py
│   │   ├── test_crawlee_error_taxonomy.py
│   │   ├── test_crawlee_request_keys.py
│   │   ├── test_facebook_engagement.py
│   │   ├── test_facebook_threads_recon_gate.py
│   │   ├── test_instagram_permalink_metadata.py
│   │   ├── tiktok
│   │   │   ├── __init__.py
│   │   │   └── test_media_resolver.py
│   │   └── youtube
│   │       ├── __init__.py
│   │       └── test_media_resolver.py
│   ├── test_api_smoke.py
│   ├── test_discussions_smoke.py
│   ├── test_dms_smoke.py
│   ├── test_fix_repo_structure_mermaid.py
│   ├── test_sync_common.py
│   ├── test_ws_realtime_smoke.py
│   ├── utils
│   │   └── test_episode_appearances_aggregation.py
│   └── vision
│       ├── test_people_count_auto_crop.py
│       └── test_text_overlay_fallback.py
└── trr_backend
    ├── __init__.py
    ├── cli
    │   ├── __init__.py
    │   ├── __main__.py
    │   └── pipeline.py
    ├── clients
    │   ├── __init__.py
    │   └── screenalytics.py
    ├── db
    │   ├── __init__.py
    │   ├── admin.py
    │   ├── connection.py
    │   ├── pg.py
    │   ├── postgrest_cache.py
    │   ├── preflight.py
    │   ├── session.py
    │   └── show_images.py
    ├── ingestion
    │   ├── __init__.py
    │   ├── cast_photo_sources.py
    │   ├── fandom_person_scraper.py
    │   ├── fandom_season_scraper.py
    │   ├── imdb_images.py
    │   ├── imdb_show_mediaindex.py
    │   ├── show_cast_matrix_scraper.py
    │   ├── show_importer.py
    │   ├── show_metadata_enricher.py
    │   ├── showinfo_overrides.py
    │   ├── shows_from_lists.py
    │   ├── tmdb_person_images.py
    │   └── tmdb_show_backfill.py
    ├── integrations
    │   ├── __init__.py
    │   ├── brandfetch.py
    │   ├── fandom.py
    │   ├── fandom_community_allowlist.txt
    │   ├── fandom_discovery.py
    │   ├── imdb
    │   │   ├── __init__.py
    │   │   ├── companycredits.py
    │   │   ├── credits_client.py
    │   │   ├── episodic_client.py
    │   │   ├── fullcredits_cast_parser.py
    │   │   ├── graphql_operations.py
    │   │   ├── graphql_persisted_client.py
    │   │   ├── list_graphql_client.py
    │   │   ├── mediaindex_images.py
    │   │   ├── person_gallery.py
    │   │   ├── title_metadata_client.py
    │   │   └── title_page_metadata.py
    │   ├── logopedia.py
    │   ├── openai_fandom_cleanup.py
    │   ├── tmdb
    │   │   ├── __init__.py
    │   │   └── client.py
    │   └── tmdb_person.py
    ├── media
    │   ├── __init__.py
    │   ├── face_crops.py
    │   ├── image_variants.py
    │   ├── s3_mirror.py
    │   └── user_uploads.py
    ├── models
    │   ├── __init__.py
    │   ├── cast_photos.py
    │   └── shows.py
    ├── pipeline
    │   ├── __init__.py
    │   ├── manifests.py
    │   ├── models.py
    │   ├── orchestrator.py
    │   ├── registry.py
    │   ├── repository.py
    │   └── stages
    │       ├── __init__.py
    │       ├── collect.py
    │       ├── deploy.py
    │       ├── enrich.py
    │       ├── mirror.py
    │       ├── resolve.py
    │       └── sync_screenalytics.py
    ├── repositories
    │   ├── __init__.py
    │   ├── brands_franchises.py
    │   ├── cast_fandom.py
    │   ├── cast_photo_tags.py
    │   ├── cast_photos.py
    │   ├── cast_tmdb.py
    │   ├── credits.py
    │   ├── episode_appearances.py
    │   ├── episode_images.py
    │   ├── episodes.py
    │   ├── imdb_series.py
    │   ├── media_assets.py
    │   ├── media_links.py
    │   ├── people.py
    │   ├── person_images.py
    │   ├── reddit_refresh.py
    │   ├── screenalytics_runs.py
    │   ├── season_fandom.py
    │   ├── season_images.py
    │   ├── seasons.py
    │   ├── show_cast.py
    │   ├── show_images.py
    │   ├── shows.py
    │   ├── social_season_analytics.py
    │   ├── sync_state.py
    │   ├── tmdb_series.py
    │   └── web_scrape_images.py
    ├── scraping
    │   ├── __init__.py
    │   ├── bravo_parser.py
    │   ├── google_news_parser.py
    │   └── url_image_scraper.py
    ├── security
    │   └── jwt.py
    ├── socials
    │   ├── __init__.py
    │   ├── crawlee_runtime
    │   │   ├── __init__.py
    │   │   ├── auth_preflight.py
    │   │   ├── config.py
    │   │   ├── error_taxonomy.py
    │   │   ├── request_keys.py
    │   │   └── runtime.py
    │   ├── facebook
    │   │   ├── __init__.py
    │   │   ├── crawlee_adapter.py
    │   │   └── scraper.py
    │   ├── instagram
    │   │   ├── __init__.py
    │   │   ├── crawlee_adapter.py
    │   │   ├── permalink_metadata.py
    │   │   └── scraper.py
    │   ├── platforms.py
    │   ├── threads
    │   │   ├── __init__.py
    │   │   ├── crawlee_adapter.py
    │   │   ├── media_resolver.py
    │   │   └── scraper.py
    │   ├── tiktok
    │   │   ├── __init__.py
    │   │   ├── crawlee_adapter.py
    │   │   ├── media_resolver.py
    │   │   └── scraper.py
    │   ├── twitter
    │   │   ├── __init__.py
    │   │   ├── crawlee_adapter.py
    │   │   └── scraper.py
    │   └── youtube
    │       ├── __init__.py
    │       ├── crawlee_adapter.py
    │       ├── media_resolver.py
    │       └── scraper.py
    ├── utils
    │   ├── __init__.py
    │   ├── array_merge.py
    │   ├── env.py
    │   └── episode_appearances.py
    └── vision
        ├── __init__.py
        └── text_overlay.py
```
