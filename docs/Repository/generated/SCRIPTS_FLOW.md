# Scripts Flow

Script entrypoints (`if __name__ == '__main__'`) and their trr_backend dependencies.

```mermaid
flowchart LR
    subgraph sg0["scripts (root)"]
        s0["backfill_bravo_video_thumbnails"]
        s1["backfill_credits"]
        s2["backfill_media_asset_variants"]
        s3["backfill_media_assets"]
        s4["backfill_tmdb_show_details"]
        s5["check_env_example"]
        s6["cleanup_expired_media_uploads"]
        s7["download_scraped_images_local"]
        s8["enrich_show_cast"]
        s9["fix_repo_structure_mermaid"]
        s10["generate_repo_mermaid"]
        s11["imdb_show_enrichment"]
        s12["import_fandom_gallery_photos"]
        s13["import_imdb_cast_episode_appearances"]
        s14["import_shows_from_lists"]
        s15["mirror_cast_photos_to_s3"]
        s16["mirror_media_assets_to_s3"]
        s17["mirror_show_images_to_s3"]
        s18["rebuild_hosted_urls"]
        s19["resolve_tmdb_ids_via_find"]
        s20["rhoslc_fandom_enrichment"]
        s21["run_show_import_job"]
        s22["sync_all_tables"]
        s23["sync_cast_batch"]
        s24["sync_cast_photos"]
        s25["sync_episode_appearances"]
        s26["sync_episodes"]
        s27["sync_networks_streaming_links"]
        s28["sync_people"]
        s29["sync_season_episode_images"]
        s30["sync_seasons"]
        s31["sync_seasons_episodes"]
        s32["sync_show_batch"]
        s33["sync_show_cast"]
        s34["sync_show_complete"]
        s35["sync_show_images"]
        s36["sync_show_logos"]
        s37["sync_shows"]
        s38["sync_shows_all"]
        s39["sync_tmdb_person_images"]
        s40["sync_tmdb_show_entities"]
        s41["sync_tmdb_watch_providers"]
        s42["validate_supabase_timeouts"]
        s43["verify_credits_parity"]
        s44["verify_media_unification"]
        s45["verify_schema"]
    end
    subgraph sg1["scripts/backfill"]
        s46["backfill_bravo_video_thumbnails"]
        s47["backfill_credits"]
        s48["backfill_media_assets"]
        s49["backfill_tmdb_show_details"]
    end
    subgraph sg2["scripts/cleanup"]
        s50["cleanup_fandom_mismatches"]
    end
    subgraph sg3["scripts/dev"]
        s51["doctor"]
    end
    subgraph sg4["scripts/enrich"]
        s52["enrich_show_cast"]
        s53["imdb_show_enrichment"]
        s54["rhoslc_fandom_enrichment"]
    end
    subgraph sg5["scripts/import"]
        s55["download_scraped_images_local"]
        s56["import_fandom_gallery_photos"]
        s57["import_imdb_cast_episode_appearances"]
        s58["import_shows_from_lists"]
        s59["run_show_import_job"]
    end
    subgraph sg6["scripts/legacy"]
        s60["test_connection"]
    end
    subgraph sg7["scripts/media"]
        s61["backfill_media_asset_variants"]
        s62["cleanup_expired_media_uploads"]
        s63["mirror_cast_photos_to_s3"]
        s64["mirror_media_assets_to_s3"]
        s65["mirror_show_images_to_s3"]
        s66["rebuild_hosted_urls"]
        s67["repair_cast_photo_hosts"]
        s68["repair_gallery_hosts"]
        s69["restore_changed_originals"]
        s70["restore_person_gallery_base_previews"]
    end
    subgraph sg8["scripts/shows"]
        s71["backfill_bravo_person_source_links"]
        s72["cleanup_invalid_person_knowledge_links"]
    end
    subgraph sg9["scripts/socials"]
        s73["backfill_bravo_missing_platform_targets"]
        s74["backfill_instagram_metadata_and_media"]
        s75["backfill_social_media_mirror_jobs"]
        s76["backfill_social_post_tokens"]
        s77["backfill_tiktok_saves"]
        s78["cleanup_youtube_false_positives"]
        s79["scrape"]
        s80["repair_social_hosted_urls"]
        s81["scrape"]
        s82["scrape"]
        s83["worker"]
        s84["scrape"]
    end
    subgraph sg10["scripts/supabase"]
        s85["generate_schema_docs"]
    end
    subgraph sg11["scripts/sync"]
        s86["resolve_tmdb_ids_via_find"]
        s87["sync_all_tables"]
        s88["sync_cast_batch"]
        s89["sync_cast_photos"]
        s90["sync_episode_appearances"]
        s91["sync_episodes"]
        s92["sync_networks_streaming_links"]
        s93["sync_people"]
        s94["sync_season_episode_images"]
        s95["sync_seasons"]
        s96["sync_seasons_episodes"]
        s97["sync_show_batch"]
        s98["sync_show_cast"]
        s99["sync_show_complete"]
        s100["sync_show_images"]
        s101["sync_show_logos"]
        s102["sync_shows"]
        s103["sync_shows_all"]
        s104["sync_tmdb_person_images"]
        s105["sync_tmdb_show_entities"]
        s106["sync_tmdb_watch_providers"]
    end
    subgraph sg12["scripts/verify"]
        s107["validate_supabase_timeouts"]
        s108["verify_credits_parity"]
        s109["verify_media_unification"]
        s110["verify_schema"]
    end
    subgraph trr["trr_backend/"]
        ingestion["ingestion"]
        integrations["integrations"]
        media["media"]
        repos["repositories"]
    end
    s47 --> repos
    s48 --> repos
    s49 --> ingestion
    s49 --> integrations
    s49 --> repos
    s52 --> ingestion
    s52 --> integrations
    s52 --> media
    s52 --> repos
    s53 --> integrations
    s54 --> integrations
    s56 --> integrations
    s56 --> repos
    s58 --> ingestion
    s58 --> integrations
    s61 --> media
    s62 --> media
    s63 --> media
    s63 --> repos
    s64 --> repos
    s65 --> media
    s65 --> repos
    s66 --> media
    s67 --> media
    s67 --> repos
    s68 --> media
    s69 --> media
    s70 --> media
    s73 --> repos
    s74 --> repos
    s75 --> repos
    s76 --> repos
    s77 --> repos
    s80 --> media
    s82 --> repos
    s83 --> repos
    s86 --> ingestion
    s86 --> integrations
    s86 --> repos
    s89 --> ingestion
    s89 --> integrations
    s89 --> media
    s89 --> repos
    s90 --> ingestion
    s90 --> integrations
    s90 --> repos
    s91 --> ingestion
    s91 --> repos
    s92 --> integrations
    s92 --> media
    s93 --> ingestion
    s93 --> integrations
    s93 --> repos
    s94 --> integrations
    s94 --> media
    s94 --> repos
    s95 --> ingestion
    s95 --> repos
    s96 --> repos
    s98 --> ingestion
    s98 --> integrations
    s98 --> repos
    s100 --> ingestion
    s100 --> media
    s100 --> repos
    s101 --> media
    s101 --> repos
    s102 --> ingestion
    s102 --> repos
    s104 --> ingestion
    s104 --> repos
    s105 --> integrations
    s105 --> media
    s105 --> repos
    s106 --> integrations
    s106 --> media
    s106 --> repos
```
