# Scripts Flow

Script entrypoints (`if __name__ == '__main__'`) and their trr_backend dependencies.

```mermaid
flowchart LR
    subgraph sg0["scripts (root)"]
        s0["apply_migration_0054"]
        s1["backfill_credits"]
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
        s18["run_pipeline"]
        s19["run_show_import_job"]
        s20["save_article_images"]
        s21["sync_all_tables"]
        s22["sync_cast_batch"]
        s23["sync_cast_photos"]
        s24["sync_episode_appearances"]
        s25["sync_episodes"]
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
        s41["verify_schema"]
    end
    subgraph sg1["scripts/supabase"]
        s42["generate_schema_docs"]
    end
    subgraph trr["trr_backend/"]
        ingestion["ingestion"]
        integrations["integrations"]
        media["media"]
        repos["repositories"]
    end
    s1 --> repos
    s2 --> repos
    s3 --> ingestion
    s3 --> integrations
    s3 --> repos
    s4 --> media
    s5 --> ingestion
    s5 --> integrations
    s5 --> media
    s5 --> repos
    s8 --> integrations
    s9 --> integrations
    s9 --> repos
    s10 --> ingestion
    s10 --> integrations
    s10 --> repos
    s11 --> ingestion
    s11 --> integrations
    s12 --> media
    s12 --> repos
    s13 --> repos
    s14 --> media
    s14 --> repos
    s15 --> media
    s16 --> ingestion
    s16 --> integrations
    s16 --> repos
    s17 --> integrations
    s20 --> integrations
    s23 --> ingestion
    s23 --> media
    s23 --> repos
    s24 --> ingestion
    s24 --> integrations
    s24 --> repos
    s25 --> ingestion
    s25 --> repos
    s26 --> ingestion
    s26 --> integrations
    s26 --> repos
    s27 --> integrations
    s27 --> media
    s27 --> repos
    s28 --> ingestion
    s28 --> repos
    s29 --> repos
    s31 --> ingestion
    s31 --> integrations
    s31 --> repos
    s33 --> ingestion
    s33 --> media
    s33 --> repos
    s34 --> ingestion
    s34 --> repos
    s36 --> ingestion
    s36 --> repos
    s37 --> integrations
    s37 --> media
    s37 --> repos
    s38 --> integrations
    s38 --> media
    s38 --> repos
```
