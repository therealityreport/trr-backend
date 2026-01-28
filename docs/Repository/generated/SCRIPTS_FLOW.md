# Scripts Flow

Script entrypoints (`if __name__ == '__main__'`) and their trr_backend dependencies.

```mermaid
flowchart LR
    subgraph sg0["scripts (root)"]
        s0["backfill_credits"]
        s1["backfill_media_assets"]
        s2["backfill_tmdb_show_details"]
        s3["cleanup_expired_media_uploads"]
        s4["enrich_show_cast"]
        s5["fix_repo_structure_mermaid"]
        s6["generate_repo_mermaid"]
        s7["imdb_show_enrichment"]
        s8["import_fandom_gallery_photos"]
        s9["import_imdb_cast_episode_appearances"]
        s10["import_shows_from_lists"]
        s11["mirror_cast_photos_to_s3"]
        s12["mirror_media_assets_to_s3"]
        s13["mirror_show_images_to_s3"]
        s14["rebuild_hosted_urls"]
        s15["resolve_tmdb_ids_via_find"]
        s16["rhoslc_fandom_enrichment"]
        s17["run_show_import_job"]
        s18["sync_all_tables"]
        s19["sync_cast_batch"]
        s20["sync_cast_photos"]
        s21["sync_episode_appearances"]
        s22["sync_episodes"]
        s23["sync_people"]
        s24["sync_season_episode_images"]
        s25["sync_seasons"]
        s26["sync_seasons_episodes"]
        s27["sync_show_batch"]
        s28["sync_show_cast"]
        s29["sync_show_complete"]
        s30["sync_show_images"]
        s31["sync_shows"]
        s32["sync_shows_all"]
        s33["sync_tmdb_person_images"]
        s34["sync_tmdb_show_entities"]
        s35["sync_tmdb_watch_providers"]
        s36["validate_supabase_timeouts"]
        s37["verify_credits_parity"]
        s38["verify_media_unification"]
        s39["verify_schema"]
    end
    subgraph sg1["scripts/backfill"]
        s40["backfill_credits"]
        s41["backfill_media_assets"]
        s42["backfill_tmdb_show_details"]
    end
    subgraph sg2["scripts/dev"]
        s43["doctor"]
    end
    subgraph sg3["scripts/enrich"]
        s44["enrich_show_cast"]
        s45["imdb_show_enrichment"]
        s46["rhoslc_fandom_enrichment"]
    end
    subgraph sg4["scripts/import"]
        s47["import_fandom_gallery_photos"]
        s48["import_imdb_cast_episode_appearances"]
        s49["import_shows_from_lists"]
        s50["run_show_import_job"]
    end
    subgraph sg5["scripts/media"]
        s51["cleanup_expired_media_uploads"]
        s52["mirror_cast_photos_to_s3"]
        s53["mirror_media_assets_to_s3"]
        s54["mirror_show_images_to_s3"]
        s55["rebuild_hosted_urls"]
    end
    subgraph sg6["scripts/supabase"]
        s56["generate_schema_docs"]
    end
    subgraph sg7["scripts/sync"]
        s57["resolve_tmdb_ids_via_find"]
        s58["sync_all_tables"]
        s59["sync_cast_batch"]
        s60["sync_cast_photos"]
        s61["sync_episode_appearances"]
        s62["sync_episodes"]
        s63["sync_people"]
        s64["sync_season_episode_images"]
        s65["sync_seasons"]
        s66["sync_seasons_episodes"]
        s67["sync_show_batch"]
        s68["sync_show_cast"]
        s69["sync_show_complete"]
        s70["sync_show_images"]
        s71["sync_shows"]
        s72["sync_shows_all"]
        s73["sync_tmdb_person_images"]
        s74["sync_tmdb_show_entities"]
        s75["sync_tmdb_watch_providers"]
    end
    subgraph sg8["scripts/verify"]
        s76["validate_supabase_timeouts"]
        s77["verify_credits_parity"]
        s78["verify_media_unification"]
        s79["verify_schema"]
    end
    subgraph trr["trr_backend/"]
        ingestion["ingestion"]
        integrations["integrations"]
        media["media"]
        repos["repositories"]
    end
    s40 --> repos
    s41 --> repos
    s42 --> ingestion
    s42 --> integrations
    s42 --> repos
    s44 --> ingestion
    s44 --> integrations
    s44 --> media
    s44 --> repos
    s45 --> integrations
    s46 --> integrations
    s47 --> integrations
    s47 --> repos
    s48 --> ingestion
    s48 --> integrations
    s48 --> repos
    s49 --> ingestion
    s49 --> integrations
    s51 --> media
    s52 --> media
    s52 --> repos
    s53 --> repos
    s54 --> media
    s54 --> repos
    s55 --> media
    s57 --> ingestion
    s57 --> integrations
    s57 --> repos
    s60 --> ingestion
    s60 --> media
    s60 --> repos
    s61 --> ingestion
    s61 --> integrations
    s61 --> repos
    s62 --> ingestion
    s62 --> repos
    s63 --> ingestion
    s63 --> integrations
    s63 --> repos
    s64 --> integrations
    s64 --> media
    s64 --> repos
    s65 --> ingestion
    s65 --> repos
    s66 --> repos
    s68 --> ingestion
    s68 --> integrations
    s68 --> repos
    s70 --> ingestion
    s70 --> media
    s70 --> repos
    s71 --> ingestion
    s71 --> repos
    s73 --> ingestion
    s73 --> repos
    s74 --> integrations
    s74 --> media
    s74 --> repos
    s75 --> integrations
    s75 --> media
    s75 --> repos
```
