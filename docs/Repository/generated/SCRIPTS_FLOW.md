# Scripts Flow

Script entrypoints (`if __name__ == '__main__'`) and their trr_backend dependencies.

```mermaid
flowchart LR
    subgraph sg0["scripts (root)"]
        s0["backfill_bravo_video_thumbnails"]
        s1["backfill_credits"]
        s2["backfill_fandom_link_discovery"]
        s3["backfill_imdb_metadata"]
        s4["backfill_media_asset_variants"]
        s5["backfill_media_assets"]
        s6["backfill_shared_social_links"]
        s7["backfill_show_overview_metadata"]
        s8["backfill_tmdb_show_details"]
        s9["bravotv_get_images"]
        s10["check_env_example"]
        s11["cleanup_expired_media_uploads"]
        s12["download_scraped_images_local"]
        s13["enrich_show_cast"]
        s14["fix_repo_structure_mermaid"]
        s15["generate_repo_mermaid"]
        s16["getty_local_server"]
        s17["getty_login_headed"]
        s18["getty_prefetch"]
        s19["getty_scrape_job"]
        s20["getty_scrape_json"]
        s21["imdb_show_enrichment"]
        s22["import_fandom_gallery_photos"]
        s23["import_imdb_cast_episode_appearances"]
        s24["import_shows_from_lists"]
        s25["mirror_cast_photos_to_s3"]
        s26["mirror_media_assets_to_s3"]
        s27["mirror_show_images_to_s3"]
        s28["rebuild_hosted_urls"]
        s29["resolve_tmdb_ids_via_find"]
        s30["rhoslc_fandom_enrichment"]
        s31["run_show_import_job"]
        s32["sync_all_tables"]
        s33["sync_cast_batch"]
        s34["sync_cast_photos"]
        s35["sync_episode_appearances"]
        s36["sync_episodes"]
        s37["sync_networks_streaming_links"]
        s38["sync_people"]
        s39["sync_season_episode_images"]
        s40["sync_seasons"]
        s41["sync_seasons_episodes"]
        s42["sync_show_batch"]
        s43["sync_show_cast"]
        s44["sync_show_complete"]
        s45["sync_show_images"]
        s46["sync_show_logos"]
        s47["sync_shows"]
        s48["sync_shows_all"]
        s49["sync_tmdb_person_images"]
        s50["sync_tmdb_show_entities"]
        s51["sync_tmdb_watch_providers"]
        s52["validate_supabase_timeouts"]
        s53["verify_credits_parity"]
        s54["verify_media_unification"]
        s55["verify_schema"]
    end
    subgraph sg1["scripts/backfill"]
        s56["backfill_bravo_video_thumbnails"]
        s57["backfill_credits"]
        s58["backfill_getty_nbcumv_metadata"]
        s59["backfill_media_assets"]
        s60["backfill_tmdb_show_details"]
        s61["repair_imdb_show_context"]
    end
    subgraph sg2["scripts/cleanup"]
        s62["cleanup_fandom_mismatches"]
        s63["cleanup_non_confessional_fandom_person_media"]
    end
    subgraph sg3["scripts/dev"]
        s64["doctor"]
    end
    subgraph sg4["scripts/enrich"]
        s65["enrich_show_cast"]
        s66["imdb_show_enrichment"]
        s67["rhoslc_fandom_enrichment"]
    end
    subgraph sg5["scripts/import"]
        s68["download_scraped_images_local"]
        s69["import_fandom_gallery_photos"]
        s70["import_imdb_cast_episode_appearances"]
        s71["import_shows_from_lists"]
        s72["run_show_import_job"]
    end
    subgraph sg6["scripts/legacy"]
        s73["test_connection"]
    end
    subgraph sg7["scripts/media"]
        s74["backfill_media_asset_variants"]
        s75["cleanup_expired_media_uploads"]
        s76["mirror_cast_photos_to_s3"]
        s77["mirror_media_assets_to_s3"]
        s78["mirror_show_images_to_s3"]
        s79["rebuild_hosted_urls"]
        s80["repair_cast_photo_hosts"]
        s81["repair_gallery_hosts"]
        s82["repair_person_getty_gallery_buckets"]
        s83["repair_person_getty_originals"]
        s84["restore_changed_originals"]
        s85["restore_person_gallery_base_previews"]
    end
    subgraph sg8["scripts/modal"]
        s86["prepare_named_secrets"]
        s87["render_cutover_commands"]
        s88["verify_modal_readiness"]
    end
    subgraph sg9["scripts/ops"]
        s89["cast_screentime_deployed_smoke"]
        s90["cast_screentime_stale_run_drill"]
        s91["socialblade_deployed_smoke"]
    end
    subgraph sg10["scripts/shows"]
        s92["backfill_bravo_person_source_links"]
        s93["cleanup_invalid_person_knowledge_links"]
        s94["normalize_entity_links_url_keys"]
    end
    subgraph sg11["scripts/socials"]
        s95["backfill_bravo_missing_platform_targets"]
        s96["backfill_instagram_metadata_and_media"]
        s97["backfill_instagram_profile_avatars"]
        s98["backfill_instagram_reel_views_full_history"]
        s99["backfill_rhoslc_s6_tags_collaborators"]
        s100["backfill_social_media_mirror_jobs"]
        s101["backfill_social_post_tokens"]
        s102["backfill_tiktok_saves"]
        s103["benchmark_bravotv"]
        s104["benchmark_sync_jobs"]
        s105["cleanup_youtube_false_positives"]
        s106["import_socialblade_seed"]
        s107["scrape"]
        s108["refresh_cookies"]
        s109["repair_instagram_single_media_urls"]
        s110["repair_social_hosted_urls"]
        s111["repair_twitter_quotes_metrics_and_comment_media"]
        s112["repair_twitter_video_thumbnails"]
        s113["repair_youtube_short_timestamps"]
        s114["retire_stale_threads_media_mirror_failures"]
        s115["run_rhoslc_threads_full_refresh"]
        s116["scrape"]
        s117["scrape"]
        s118["verify_shared_account_catalog"]
        s119["worker"]
        s120["scrape"]
    end
    subgraph sg12["scripts/supabase"]
        s121["generate_schema_docs"]
    end
    subgraph sg13["scripts/sync"]
        s122["resolve_tmdb_ids_via_find"]
        s123["sync_all_tables"]
        s124["sync_bravotv_galleries"]
        s125["sync_cast_batch"]
        s126["sync_cast_photos"]
        s127["sync_episode_appearances"]
        s128["sync_episodes"]
        s129["sync_networks_streaming_links"]
        s130["sync_people"]
        s131["sync_season_episode_images"]
        s132["sync_seasons"]
        s133["sync_seasons_episodes"]
        s134["sync_show_batch"]
        s135["sync_show_cast"]
        s136["sync_show_complete"]
        s137["sync_show_images"]
        s138["sync_show_logos"]
        s139["sync_shows"]
        s140["sync_shows_all"]
        s141["sync_tmdb_person_images"]
        s142["sync_tmdb_show_entities"]
        s143["sync_tmdb_watch_providers"]
    end
    subgraph sg14["scripts/verify"]
        s144["validate_supabase_timeouts"]
        s145["verify_credits_parity"]
        s146["verify_media_unification"]
        s147["verify_schema"]
    end
    subgraph sg15["scripts/workers"]
        s148["admin_operations_worker"]
        s149["google_news_worker"]
        s150["reddit_refresh_worker"]
    end
    subgraph trr["trr_backend/"]
        ingestion["ingestion"]
        integrations["integrations"]
        media["media"]
        repos["repositories"]
    end
    s7 --> integrations
    s16 --> integrations
    s18 --> integrations
    s19 --> integrations
    s20 --> integrations
    s57 --> repos
    s59 --> media
    s59 --> repos
    s60 --> ingestion
    s60 --> integrations
    s60 --> repos
    s63 --> ingestion
    s63 --> media
    s65 --> ingestion
    s65 --> integrations
    s65 --> media
    s65 --> repos
    s66 --> integrations
    s67 --> integrations
    s69 --> ingestion
    s69 --> integrations
    s69 --> repos
    s71 --> ingestion
    s71 --> integrations
    s74 --> media
    s75 --> media
    s76 --> media
    s76 --> repos
    s77 --> repos
    s78 --> media
    s78 --> repos
    s79 --> media
    s80 --> media
    s80 --> repos
    s81 --> media
    s83 --> integrations
    s84 --> media
    s85 --> media
    s91 --> repos
    s95 --> repos
    s96 --> repos
    s97 --> repos
    s98 --> repos
    s99 --> repos
    s100 --> repos
    s101 --> repos
    s102 --> repos
    s104 --> repos
    s106 --> repos
    s107 --> repos
    s108 --> repos
    s110 --> media
    s110 --> repos
    s111 --> repos
    s115 --> repos
    s116 --> repos
    s117 --> repos
    s118 --> repos
    s119 --> repos
    s122 --> ingestion
    s122 --> integrations
    s122 --> repos
    s126 --> ingestion
    s126 --> integrations
    s126 --> media
    s126 --> repos
    s127 --> ingestion
    s127 --> integrations
    s127 --> repos
    s128 --> ingestion
    s128 --> repos
    s129 --> integrations
    s129 --> media
    s130 --> ingestion
    s130 --> integrations
    s130 --> repos
    s131 --> integrations
    s131 --> media
    s131 --> repos
    s132 --> ingestion
    s132 --> repos
    s133 --> repos
    s135 --> ingestion
    s135 --> integrations
    s135 --> repos
    s137 --> ingestion
    s137 --> media
    s137 --> repos
    s138 --> media
    s138 --> repos
    s139 --> ingestion
    s139 --> repos
    s141 --> ingestion
    s141 --> repos
    s142 --> integrations
    s142 --> media
    s142 --> repos
    s143 --> integrations
    s143 --> media
    s143 --> repos
    s150 --> repos
```
