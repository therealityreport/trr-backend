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
    subgraph sg5["scripts/legacy"]
        s51["test_connection"]
    end
    subgraph sg6["scripts/media"]
        s52["cleanup_expired_media_uploads"]
        s53["mirror_cast_photos_to_s3"]
        s54["mirror_media_assets_to_s3"]
        s55["mirror_show_images_to_s3"]
        s56["rebuild_hosted_urls"]
    end
    subgraph sg7["scripts/supabase"]
        s57["generate_schema_docs"]
    end
    subgraph sg8["scripts/sync"]
        s58["resolve_tmdb_ids_via_find"]
        s59["sync_all_tables"]
        s60["sync_cast_batch"]
        s61["sync_cast_photos"]
        s62["sync_episode_appearances"]
        s63["sync_episodes"]
        s64["sync_people"]
        s65["sync_season_episode_images"]
        s66["sync_seasons"]
        s67["sync_seasons_episodes"]
        s68["sync_show_batch"]
        s69["sync_show_cast"]
        s70["sync_show_complete"]
        s71["sync_show_images"]
        s72["sync_shows"]
        s73["sync_shows_all"]
        s74["sync_tmdb_person_images"]
        s75["sync_tmdb_show_entities"]
        s76["sync_tmdb_watch_providers"]
    end
    subgraph sg9["scripts/verify"]
        s77["validate_supabase_timeouts"]
        s78["verify_credits_parity"]
        s79["verify_media_unification"]
        s80["verify_schema"]
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
    s52 --> media
    s53 --> media
    s53 --> repos
    s54 --> repos
    s55 --> media
    s55 --> repos
    s56 --> media
    s58 --> ingestion
    s58 --> integrations
    s58 --> repos
    s61 --> ingestion
    s61 --> media
    s61 --> repos
    s62 --> ingestion
    s62 --> integrations
    s62 --> repos
    s63 --> ingestion
    s63 --> repos
    s64 --> ingestion
    s64 --> integrations
    s64 --> repos
    s65 --> integrations
    s65 --> media
    s65 --> repos
    s66 --> ingestion
    s66 --> repos
    s67 --> repos
    s69 --> ingestion
    s69 --> integrations
    s69 --> repos
    s71 --> ingestion
    s71 --> media
    s71 --> repos
    s72 --> ingestion
    s72 --> repos
    s74 --> ingestion
    s74 --> repos
    s75 --> integrations
    s75 --> media
    s75 --> repos
    s76 --> integrations
    s76 --> media
    s76 --> repos
```
