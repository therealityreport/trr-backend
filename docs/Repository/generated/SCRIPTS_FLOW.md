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
        s49["backfill_media_assets"]
        s50["backfill_tmdb_show_details"]
        s51["repair_imdb_show_context"]
    end
    subgraph sg2["scripts/cleanup"]
        s52["cleanup_fandom_mismatches"]
    end
    subgraph sg3["scripts/dev"]
        s53["doctor"]
    end
    subgraph sg4["scripts/enrich"]
        s54["enrich_show_cast"]
        s55["imdb_show_enrichment"]
        s56["rhoslc_fandom_enrichment"]
    end
    subgraph sg5["scripts/import"]
        s57["download_scraped_images_local"]
        s58["import_fandom_gallery_photos"]
        s59["import_imdb_cast_episode_appearances"]
        s60["import_shows_from_lists"]
        s61["run_show_import_job"]
    end
    subgraph sg6["scripts/legacy"]
        s62["test_connection"]
    end
    subgraph sg7["scripts/media"]
        s63["backfill_media_asset_variants"]
        s64["cleanup_expired_media_uploads"]
        s65["mirror_cast_photos_to_s3"]
        s66["mirror_media_assets_to_s3"]
        s67["mirror_show_images_to_s3"]
        s68["rebuild_hosted_urls"]
        s69["repair_cast_photo_hosts"]
        s70["repair_gallery_hosts"]
        s71["restore_changed_originals"]
        s72["restore_person_gallery_base_previews"]
    end
    subgraph sg8["scripts/modal"]
        s73["prepare_named_secrets"]
        s74["render_cutover_commands"]
        s75["verify_modal_readiness"]
    end
    subgraph sg9["scripts/ops"]
        s76["aws_teardown_pass"]
    end
    subgraph sg10["scripts/render"]
        s77["sync_render_service_from_aws"]
    end
    subgraph sg11["scripts/shows"]
        s78["backfill_bravo_person_source_links"]
        s79["cleanup_invalid_person_knowledge_links"]
        s80["normalize_entity_links_url_keys"]
    end
    subgraph sg12["scripts/socials"]
        s81["backfill_bravo_missing_platform_targets"]
        s82["backfill_instagram_metadata_and_media"]
        s83["backfill_instagram_reel_views_full_history"]
        s84["backfill_rhoslc_s6_tags_collaborators"]
        s85["backfill_social_media_mirror_jobs"]
        s86["backfill_social_post_tokens"]
        s87["backfill_tiktok_saves"]
        s88["benchmark_sync_jobs"]
        s89["cleanup_youtube_false_positives"]
        s90["scrape"]
        s91["refresh_cookies"]
        s92["repair_social_hosted_urls"]
        s93["repair_twitter_quotes_metrics_and_comment_media"]
        s94["repair_twitter_video_thumbnails"]
        s95["run_rhoslc_threads_full_refresh"]
        s96["scrape"]
        s97["scrape"]
        s98["worker"]
        s99["scrape"]
    end
    subgraph sg13["scripts/storage"]
        s100["sync_bucket_to_r2"]
        s101["verify_bucket_sync"]
    end
    subgraph sg14["scripts/supabase"]
        s102["generate_schema_docs"]
    end
    subgraph sg15["scripts/sync"]
        s103["resolve_tmdb_ids_via_find"]
        s104["sync_all_tables"]
        s105["sync_cast_batch"]
        s106["sync_cast_photos"]
        s107["sync_episode_appearances"]
        s108["sync_episodes"]
        s109["sync_networks_streaming_links"]
        s110["sync_people"]
        s111["sync_season_episode_images"]
        s112["sync_seasons"]
        s113["sync_seasons_episodes"]
        s114["sync_show_batch"]
        s115["sync_show_cast"]
        s116["sync_show_complete"]
        s117["sync_show_images"]
        s118["sync_show_logos"]
        s119["sync_shows"]
        s120["sync_shows_all"]
        s121["sync_tmdb_person_images"]
        s122["sync_tmdb_show_entities"]
        s123["sync_tmdb_watch_providers"]
    end
    subgraph sg16["scripts/verify"]
        s124["validate_supabase_timeouts"]
        s125["verify_credits_parity"]
        s126["verify_media_unification"]
        s127["verify_schema"]
    end
    subgraph sg17["scripts/workers"]
        s128["admin_operations_worker"]
        s129["google_news_worker"]
        s130["reddit_refresh_worker"]
    end
    subgraph trr["trr_backend/"]
        ingestion["ingestion"]
        integrations["integrations"]
        media["media"]
        repos["repositories"]
    end
    s48 --> repos
    s49 --> media
    s49 --> repos
    s50 --> ingestion
    s50 --> integrations
    s50 --> repos
    s54 --> ingestion
    s54 --> integrations
    s54 --> media
    s54 --> repos
    s55 --> integrations
    s56 --> integrations
    s58 --> integrations
    s58 --> repos
    s60 --> ingestion
    s60 --> integrations
    s63 --> media
    s64 --> media
    s65 --> media
    s65 --> repos
    s66 --> repos
    s67 --> media
    s67 --> repos
    s68 --> media
    s69 --> media
    s69 --> repos
    s70 --> media
    s71 --> media
    s72 --> media
    s81 --> repos
    s82 --> repos
    s83 --> repos
    s84 --> repos
    s85 --> repos
    s86 --> repos
    s87 --> repos
    s88 --> repos
    s90 --> repos
    s91 --> repos
    s92 --> media
    s93 --> repos
    s95 --> repos
    s96 --> repos
    s97 --> repos
    s98 --> repos
    s103 --> ingestion
    s103 --> integrations
    s103 --> repos
    s106 --> ingestion
    s106 --> integrations
    s106 --> media
    s106 --> repos
    s107 --> ingestion
    s107 --> integrations
    s107 --> repos
    s108 --> ingestion
    s108 --> repos
    s109 --> integrations
    s109 --> media
    s110 --> ingestion
    s110 --> integrations
    s110 --> repos
    s111 --> integrations
    s111 --> media
    s111 --> repos
    s112 --> ingestion
    s112 --> repos
    s113 --> repos
    s115 --> ingestion
    s115 --> integrations
    s115 --> repos
    s117 --> ingestion
    s117 --> media
    s117 --> repos
    s118 --> media
    s118 --> repos
    s119 --> ingestion
    s119 --> repos
    s121 --> ingestion
    s121 --> repos
    s122 --> integrations
    s122 --> media
    s122 --> repos
    s123 --> integrations
    s123 --> media
    s123 --> repos
    s130 --> repos
```
