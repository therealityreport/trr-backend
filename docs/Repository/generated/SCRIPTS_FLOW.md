# Scripts Flow

Script entrypoints (`if __name__ == '__main__'`) and their trr_backend dependencies.

```mermaid
flowchart LR
    subgraph sg0["scripts (root)"]
        s0["backfill_credits"]
        s1["backfill_media_asset_variants"]
        s2["backfill_media_assets"]
        s3["backfill_tmdb_show_details"]
        s4["check_env_example"]
        s5["cleanup_expired_media_uploads"]
        s6["enrich_show_cast"]
        s7["fix_repo_structure_mermaid"]
        s8["generate_repo_mermaid"]
        s9["imdb_show_enrichment"]
        s10["import_fandom_gallery_photos"]
        s11["import_imdb_cast_episode_appearances"]
        s12["import_shows_from_lists"]
        s13["mirror_cast_photos_to_s3"]
        s14["mirror_media_assets_to_s3"]
        s15["mirror_show_images_to_s3"]
        s16["rebuild_hosted_urls"]
        s17["resolve_tmdb_ids_via_find"]
        s18["rhoslc_fandom_enrichment"]
        s19["run_show_import_job"]
        s20["sync_all_tables"]
        s21["sync_cast_batch"]
        s22["sync_cast_photos"]
        s23["sync_episode_appearances"]
        s24["sync_episodes"]
        s25["sync_networks_streaming_links"]
        s26["sync_people"]
        s27["sync_season_episode_images"]
        s28["sync_seasons"]
        s29["sync_seasons_episodes"]
        s30["sync_show_batch"]
        s31["sync_show_cast"]
        s32["sync_show_complete"]
        s33["sync_show_images"]
        s34["sync_shows"]
        s35["sync_shows_all"]
        s36["sync_tmdb_person_images"]
        s37["sync_tmdb_show_entities"]
        s38["sync_tmdb_watch_providers"]
        s39["validate_supabase_timeouts"]
        s40["verify_credits_parity"]
        s41["verify_media_unification"]
        s42["verify_schema"]
    end
    subgraph sg1["scripts/backfill"]
        s43["backfill_credits"]
        s44["backfill_media_assets"]
        s45["backfill_tmdb_show_details"]
    end
    subgraph sg2["scripts/cleanup"]
        s46["cleanup_fandom_mismatches"]
    end
    subgraph sg3["scripts/dev"]
        s47["doctor"]
    end
    subgraph sg4["scripts/enrich"]
        s48["enrich_show_cast"]
        s49["imdb_show_enrichment"]
        s50["rhoslc_fandom_enrichment"]
    end
    subgraph sg5["scripts/import"]
        s51["import_fandom_gallery_photos"]
        s52["import_imdb_cast_episode_appearances"]
        s53["import_shows_from_lists"]
        s54["run_show_import_job"]
    end
    subgraph sg6["scripts/legacy"]
        s55["test_connection"]
    end
    subgraph sg7["scripts/media"]
        s56["backfill_media_asset_variants"]
        s57["cleanup_expired_media_uploads"]
        s58["mirror_cast_photos_to_s3"]
        s59["mirror_media_assets_to_s3"]
        s60["mirror_show_images_to_s3"]
        s61["rebuild_hosted_urls"]
        s62["repair_cast_photo_hosts"]
    end
    subgraph sg8["scripts/shows"]
        s63["backfill_bravo_person_source_links"]
        s64["cleanup_invalid_person_knowledge_links"]
    end
    subgraph sg9["scripts/socials"]
        s65["backfill_instagram_metadata_and_media"]
        s66["cleanup_youtube_false_positives"]
        s67["scrape"]
        s68["scrape"]
        s69["scrape"]
        s70["worker"]
        s71["scrape"]
    end
    subgraph sg10["scripts/supabase"]
        s72["generate_schema_docs"]
    end
    subgraph sg11["scripts/sync"]
        s73["resolve_tmdb_ids_via_find"]
        s74["sync_all_tables"]
        s75["sync_cast_batch"]
        s76["sync_cast_photos"]
        s77["sync_episode_appearances"]
        s78["sync_episodes"]
        s79["sync_networks_streaming_links"]
        s80["sync_people"]
        s81["sync_season_episode_images"]
        s82["sync_seasons"]
        s83["sync_seasons_episodes"]
        s84["sync_show_batch"]
        s85["sync_show_cast"]
        s86["sync_show_complete"]
        s87["sync_show_images"]
        s88["sync_shows"]
        s89["sync_shows_all"]
        s90["sync_tmdb_person_images"]
        s91["sync_tmdb_show_entities"]
        s92["sync_tmdb_watch_providers"]
    end
    subgraph sg12["scripts/verify"]
        s93["validate_supabase_timeouts"]
        s94["verify_credits_parity"]
        s95["verify_media_unification"]
        s96["verify_schema"]
    end
    subgraph trr["trr_backend/"]
        ingestion["ingestion"]
        integrations["integrations"]
        media["media"]
        repos["repositories"]
    end
    s43 --> repos
    s44 --> repos
    s45 --> ingestion
    s45 --> integrations
    s45 --> repos
    s48 --> ingestion
    s48 --> integrations
    s48 --> media
    s48 --> repos
    s49 --> integrations
    s50 --> integrations
    s51 --> integrations
    s51 --> repos
    s53 --> ingestion
    s53 --> integrations
    s56 --> media
    s57 --> media
    s58 --> media
    s58 --> repos
    s59 --> repos
    s60 --> media
    s60 --> repos
    s61 --> media
    s62 --> media
    s62 --> repos
    s65 --> repos
    s70 --> repos
    s73 --> ingestion
    s73 --> integrations
    s73 --> repos
    s76 --> ingestion
    s76 --> integrations
    s76 --> media
    s76 --> repos
    s77 --> ingestion
    s77 --> integrations
    s77 --> repos
    s78 --> ingestion
    s78 --> repos
    s79 --> integrations
    s79 --> media
    s80 --> ingestion
    s80 --> integrations
    s80 --> repos
    s81 --> integrations
    s81 --> media
    s81 --> repos
    s82 --> ingestion
    s82 --> repos
    s83 --> repos
    s85 --> ingestion
    s85 --> integrations
    s85 --> repos
    s87 --> ingestion
    s87 --> media
    s87 --> repos
    s88 --> ingestion
    s88 --> repos
    s90 --> ingestion
    s90 --> repos
    s91 --> integrations
    s91 --> media
    s91 --> repos
    s92 --> integrations
    s92 --> media
    s92 --> repos
```
