# Scripts Flow

Script entrypoints (`if __name__ == '__main__'`) and their trr_backend dependencies.

```mermaid
flowchart LR
    subgraph sg0["scripts (root)"]
        s0["backfill_credits"]
        s1["backfill_media_asset_variants"]
        s2["backfill_media_assets"]
        s3["backfill_tmdb_show_details"]
        s4["cleanup_expired_media_uploads"]
        s5["enrich_show_cast"]
        s6["fix_repo_structure_mermaid"]
        s7["generate_repo_mermaid"]
        s8["imdb_show_enrichment"]
        s9["import_fandom_gallery_photos"]
        s10["import_imdb_cast_episode_appearances"]
        s11["import_shows_from_lists"]
        s12["mirror_cast_photos_to_s3"]
        s13["mirror_media_assets_to_s3"]
        s14["mirror_show_images_to_s3"]
        s15["rebuild_hosted_urls"]
        s16["resolve_tmdb_ids_via_find"]
        s17["rhoslc_fandom_enrichment"]
        s18["run_show_import_job"]
        s19["sync_all_tables"]
        s20["sync_cast_batch"]
        s21["sync_cast_photos"]
        s22["sync_episode_appearances"]
        s23["sync_episodes"]
        s24["sync_people"]
        s25["sync_season_episode_images"]
        s26["sync_seasons"]
        s27["sync_seasons_episodes"]
        s28["sync_show_batch"]
        s29["sync_show_cast"]
        s30["sync_show_complete"]
        s31["sync_show_images"]
        s32["sync_shows"]
        s33["sync_shows_all"]
        s34["sync_tmdb_person_images"]
        s35["sync_tmdb_show_entities"]
        s36["sync_tmdb_watch_providers"]
        s37["validate_supabase_timeouts"]
        s38["verify_credits_parity"]
        s39["verify_media_unification"]
        s40["verify_schema"]
    end
    subgraph sg1["scripts/backfill"]
        s41["backfill_credits"]
        s42["backfill_media_assets"]
        s43["backfill_tmdb_show_details"]
    end
    subgraph sg2["scripts/cleanup"]
        s44["cleanup_fandom_mismatches"]
    end
    subgraph sg3["scripts/dev"]
        s45["doctor"]
    end
    subgraph sg4["scripts/enrich"]
        s46["enrich_show_cast"]
        s47["imdb_show_enrichment"]
        s48["rhoslc_fandom_enrichment"]
    end
    subgraph sg5["scripts/import"]
        s49["import_fandom_gallery_photos"]
        s50["import_imdb_cast_episode_appearances"]
        s51["import_shows_from_lists"]
        s52["run_show_import_job"]
    end
    subgraph sg6["scripts/legacy"]
        s53["test_connection"]
    end
    subgraph sg7["scripts/media"]
        s54["backfill_media_asset_variants"]
        s55["cleanup_expired_media_uploads"]
        s56["mirror_cast_photos_to_s3"]
        s57["mirror_media_assets_to_s3"]
        s58["mirror_show_images_to_s3"]
        s59["rebuild_hosted_urls"]
        s60["repair_cast_photo_hosts"]
    end
    subgraph sg8["scripts/socials"]
        s61["scrape"]
        s62["scrape"]
        s63["scrape"]
        s64["worker"]
        s65["scrape"]
    end
    subgraph sg9["scripts/supabase"]
        s66["generate_schema_docs"]
    end
    subgraph sg10["scripts/sync"]
        s67["resolve_tmdb_ids_via_find"]
        s68["sync_all_tables"]
        s69["sync_cast_batch"]
        s70["sync_cast_photos"]
        s71["sync_episode_appearances"]
        s72["sync_episodes"]
        s73["sync_people"]
        s74["sync_season_episode_images"]
        s75["sync_seasons"]
        s76["sync_seasons_episodes"]
        s77["sync_show_batch"]
        s78["sync_show_cast"]
        s79["sync_show_complete"]
        s80["sync_show_images"]
        s81["sync_shows"]
        s82["sync_shows_all"]
        s83["sync_tmdb_person_images"]
        s84["sync_tmdb_show_entities"]
        s85["sync_tmdb_watch_providers"]
    end
    subgraph sg11["scripts/verify"]
        s86["validate_supabase_timeouts"]
        s87["verify_credits_parity"]
        s88["verify_media_unification"]
        s89["verify_schema"]
    end
    subgraph trr["trr_backend/"]
        ingestion["ingestion"]
        integrations["integrations"]
        media["media"]
        repos["repositories"]
    end
    s41 --> repos
    s42 --> repos
    s43 --> ingestion
    s43 --> integrations
    s43 --> repos
    s46 --> ingestion
    s46 --> integrations
    s46 --> media
    s46 --> repos
    s47 --> integrations
    s48 --> integrations
    s49 --> integrations
    s49 --> repos
    s51 --> ingestion
    s51 --> integrations
    s54 --> media
    s55 --> media
    s56 --> media
    s56 --> repos
    s57 --> repos
    s58 --> media
    s58 --> repos
    s59 --> media
    s60 --> media
    s60 --> repos
    s64 --> repos
    s67 --> ingestion
    s67 --> integrations
    s67 --> repos
    s70 --> ingestion
    s70 --> integrations
    s70 --> media
    s70 --> repos
    s71 --> ingestion
    s71 --> integrations
    s71 --> repos
    s72 --> ingestion
    s72 --> repos
    s73 --> ingestion
    s73 --> integrations
    s73 --> repos
    s74 --> integrations
    s74 --> media
    s74 --> repos
    s75 --> ingestion
    s75 --> repos
    s76 --> repos
    s78 --> ingestion
    s78 --> integrations
    s78 --> repos
    s80 --> ingestion
    s80 --> media
    s80 --> repos
    s81 --> ingestion
    s81 --> repos
    s83 --> ingestion
    s83 --> repos
    s84 --> integrations
    s84 --> media
    s84 --> repos
    s85 --> integrations
    s85 --> media
    s85 --> repos
```
