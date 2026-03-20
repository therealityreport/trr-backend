# Scripts Flow

Script entrypoints (`if __name__ == '__main__'`) and their trr_backend dependencies.

```mermaid
flowchart LR
    subgraph sg0["scripts (root)"]
        s0["backfill_bravo_video_thumbnails"]
        s1["backfill_credits"]
        s2["backfill_imdb_metadata"]
        s3["backfill_media_asset_variants"]
        s4["backfill_media_assets"]
        s5["backfill_tmdb_show_details"]
        s6["check_env_example"]
        s7["cleanup_expired_media_uploads"]
        s8["download_scraped_images_local"]
        s9["enrich_show_cast"]
        s10["fix_repo_structure_mermaid"]
        s11["generate_repo_mermaid"]
        s12["imdb_show_enrichment"]
        s13["import_fandom_gallery_photos"]
        s14["import_imdb_cast_episode_appearances"]
        s15["import_shows_from_lists"]
        s16["mirror_cast_photos_to_s3"]
        s17["mirror_media_assets_to_s3"]
        s18["mirror_show_images_to_s3"]
        s19["rebuild_hosted_urls"]
        s20["resolve_tmdb_ids_via_find"]
        s21["rhoslc_fandom_enrichment"]
        s22["run_show_import_job"]
        s23["sync_all_tables"]
        s24["sync_cast_batch"]
        s25["sync_cast_photos"]
        s26["sync_episode_appearances"]
        s27["sync_episodes"]
        s28["sync_networks_streaming_links"]
        s29["sync_people"]
        s30["sync_season_episode_images"]
        s31["sync_seasons"]
        s32["sync_seasons_episodes"]
        s33["sync_show_batch"]
        s34["sync_show_cast"]
        s35["sync_show_complete"]
        s36["sync_show_images"]
        s37["sync_show_logos"]
        s38["sync_shows"]
        s39["sync_shows_all"]
        s40["sync_tmdb_person_images"]
        s41["sync_tmdb_show_entities"]
        s42["sync_tmdb_watch_providers"]
        s43["validate_supabase_timeouts"]
        s44["verify_credits_parity"]
        s45["verify_media_unification"]
        s46["verify_schema"]
    end
    subgraph sg1["scripts/backfill"]
        s47["backfill_bravo_video_thumbnails"]
        s48["backfill_credits"]
        s49["backfill_getty_nbcumv_metadata"]
        s50["backfill_media_assets"]
        s51["backfill_tmdb_show_details"]
        s52["repair_imdb_show_context"]
    end
    subgraph sg2["scripts/cleanup"]
        s53["cleanup_fandom_mismatches"]
        s54["cleanup_non_confessional_fandom_person_media"]
    end
    subgraph sg3["scripts/dev"]
        s55["doctor"]
    end
    subgraph sg4["scripts/enrich"]
        s56["enrich_show_cast"]
        s57["imdb_show_enrichment"]
        s58["rhoslc_fandom_enrichment"]
    end
    subgraph sg5["scripts/import"]
        s59["download_scraped_images_local"]
        s60["import_fandom_gallery_photos"]
        s61["import_imdb_cast_episode_appearances"]
        s62["import_shows_from_lists"]
        s63["run_show_import_job"]
    end
    subgraph sg6["scripts/legacy"]
        s64["test_connection"]
    end
    subgraph sg7["scripts/media"]
        s65["backfill_media_asset_variants"]
        s66["cleanup_expired_media_uploads"]
        s67["mirror_cast_photos_to_s3"]
        s68["mirror_media_assets_to_s3"]
        s69["mirror_show_images_to_s3"]
        s70["rebuild_hosted_urls"]
        s71["repair_cast_photo_hosts"]
        s72["repair_gallery_hosts"]
        s73["restore_changed_originals"]
        s74["restore_person_gallery_base_previews"]
    end
    subgraph sg8["scripts/modal"]
        s75["prepare_named_secrets"]
        s76["render_cutover_commands"]
        s77["verify_modal_readiness"]
    end
    subgraph sg9["scripts/ops"]
        s78["cast_screentime_deployed_smoke"]
        s79["cast_screentime_stale_run_drill"]
        s80["socialblade_deployed_smoke"]
    end
    subgraph sg10["scripts/shows"]
        s81["backfill_bravo_person_source_links"]
        s82["cleanup_invalid_person_knowledge_links"]
        s83["normalize_entity_links_url_keys"]
    end
    subgraph sg11["scripts/socials"]
        s84["backfill_bravo_missing_platform_targets"]
        s85["backfill_instagram_metadata_and_media"]
        s86["backfill_instagram_profile_avatars"]
        s87["backfill_instagram_reel_views_full_history"]
        s88["backfill_rhoslc_s6_tags_collaborators"]
        s89["backfill_social_media_mirror_jobs"]
        s90["backfill_social_post_tokens"]
        s91["backfill_tiktok_saves"]
        s92["benchmark_sync_jobs"]
        s93["cleanup_youtube_false_positives"]
        s94["import_socialblade_seed"]
        s95["scrape"]
        s96["refresh_cookies"]
        s97["repair_instagram_single_media_urls"]
        s98["repair_social_hosted_urls"]
        s99["repair_twitter_quotes_metrics_and_comment_media"]
        s100["repair_twitter_video_thumbnails"]
        s101["repair_youtube_short_timestamps"]
        s102["retire_stale_threads_media_mirror_failures"]
        s103["run_rhoslc_threads_full_refresh"]
        s104["scrape"]
        s105["scrape"]
        s106["verify_shared_account_catalog"]
        s107["worker"]
        s108["scrape"]
    end
    subgraph sg12["scripts/supabase"]
        s109["generate_schema_docs"]
    end
    subgraph sg13["scripts/sync"]
        s110["resolve_tmdb_ids_via_find"]
        s111["sync_all_tables"]
        s112["sync_bravotv_galleries"]
        s113["sync_cast_batch"]
        s114["sync_cast_photos"]
        s115["sync_episode_appearances"]
        s116["sync_episodes"]
        s117["sync_networks_streaming_links"]
        s118["sync_people"]
        s119["sync_season_episode_images"]
        s120["sync_seasons"]
        s121["sync_seasons_episodes"]
        s122["sync_show_batch"]
        s123["sync_show_cast"]
        s124["sync_show_complete"]
        s125["sync_show_images"]
        s126["sync_show_logos"]
        s127["sync_shows"]
        s128["sync_shows_all"]
        s129["sync_tmdb_person_images"]
        s130["sync_tmdb_show_entities"]
        s131["sync_tmdb_watch_providers"]
    end
    subgraph sg14["scripts/verify"]
        s132["validate_supabase_timeouts"]
        s133["verify_credits_parity"]
        s134["verify_media_unification"]
        s135["verify_schema"]
    end
    subgraph sg15["scripts/workers"]
        s136["admin_operations_worker"]
        s137["google_news_worker"]
        s138["reddit_refresh_worker"]
    end
    subgraph trr["trr_backend/"]
        ingestion["ingestion"]
        integrations["integrations"]
        media["media"]
        repos["repositories"]
    end
    s48 --> repos
    s50 --> media
    s50 --> repos
    s51 --> ingestion
    s51 --> integrations
    s51 --> repos
    s54 --> ingestion
    s54 --> media
    s56 --> ingestion
    s56 --> integrations
    s56 --> media
    s56 --> repos
    s57 --> integrations
    s58 --> integrations
    s60 --> ingestion
    s60 --> integrations
    s60 --> repos
    s62 --> ingestion
    s62 --> integrations
    s65 --> media
    s66 --> media
    s67 --> media
    s67 --> repos
    s68 --> repos
    s69 --> media
    s69 --> repos
    s70 --> media
    s71 --> media
    s71 --> repos
    s72 --> media
    s73 --> media
    s74 --> media
    s80 --> repos
    s84 --> repos
    s85 --> repos
    s86 --> repos
    s87 --> repos
    s88 --> repos
    s89 --> repos
    s90 --> repos
    s91 --> repos
    s92 --> repos
    s94 --> repos
    s95 --> repos
    s96 --> repos
    s98 --> media
    s98 --> repos
    s99 --> repos
    s103 --> repos
    s104 --> repos
    s105 --> repos
    s106 --> repos
    s107 --> repos
    s110 --> ingestion
    s110 --> integrations
    s110 --> repos
    s114 --> ingestion
    s114 --> integrations
    s114 --> media
    s114 --> repos
    s115 --> ingestion
    s115 --> integrations
    s115 --> repos
    s116 --> ingestion
    s116 --> repos
    s117 --> integrations
    s117 --> media
    s118 --> ingestion
    s118 --> integrations
    s118 --> repos
    s119 --> integrations
    s119 --> media
    s119 --> repos
    s120 --> ingestion
    s120 --> repos
    s121 --> repos
    s123 --> ingestion
    s123 --> integrations
    s123 --> repos
    s125 --> ingestion
    s125 --> media
    s125 --> repos
    s126 --> media
    s126 --> repos
    s127 --> ingestion
    s127 --> repos
    s129 --> ingestion
    s129 --> repos
    s130 --> integrations
    s130 --> media
    s130 --> repos
    s131 --> integrations
    s131 --> media
    s131 --> repos
    s138 --> repos
```
