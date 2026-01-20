# Scripts Flow

Script entrypoints (`if __name__ == '__main__'`) and their trr_backend dependencies.

```mermaid
flowchart LR
    subgraph sg0["scripts (root)"]
        s0["apply_migration_0054"]
        s1["backfill_tmdb_show_details"]
        s2["enrich_show_cast"]
        s3["fix_repo_structure_mermaid"]
        s4["generate_repo_mermaid"]
        s5["imdb_show_enrichment"]
        s6["import_fandom_gallery_photos"]
        s7["import_imdb_cast_episode_appearances"]
        s8["import_shows_from_lists"]
        s9["mirror_cast_photos_to_s3"]
        s10["mirror_show_images_to_s3"]
        s11["rebuild_hosted_urls"]
        s12["resolve_tmdb_ids_via_find"]
        s13["rhoslc_fandom_enrichment"]
        s14["run_pipeline"]
        s15["run_show_import_job"]
        s16["sync_all_tables"]
        s17["sync_cast_batch"]
        s18["sync_cast_photos"]
        s19["sync_episode_appearances"]
        s20["sync_episodes"]
        s21["sync_people"]
        s22["sync_season_episode_images"]
        s23["sync_seasons"]
        s24["sync_seasons_episodes"]
        s25["sync_show_batch"]
        s26["sync_show_cast"]
        s27["sync_show_complete"]
        s28["sync_show_images"]
        s29["sync_shows"]
        s30["sync_shows_all"]
        s31["sync_tmdb_person_images"]
        s32["sync_tmdb_show_entities"]
        s33["sync_tmdb_watch_providers"]
        s34["validate_supabase_timeouts"]
        s35["verify_schema"]
    end
    subgraph sg1["scripts/supabase"]
        s36["generate_schema_docs"]
    end
    subgraph trr["trr_backend/"]
        ingestion["ingestion"]
        integrations["integrations"]
        media["media"]
        repos["repositories"]
    end
    s1 --> ingestion
    s1 --> integrations
    s1 --> repos
    s2 --> ingestion
    s2 --> integrations
    s2 --> media
    s2 --> repos
    s5 --> integrations
    s6 --> integrations
    s6 --> repos
    s7 --> ingestion
    s7 --> integrations
    s7 --> repos
    s8 --> ingestion
    s8 --> integrations
    s9 --> media
    s9 --> repos
    s10 --> media
    s10 --> repos
    s11 --> media
    s12 --> ingestion
    s12 --> integrations
    s12 --> repos
    s13 --> integrations
    s18 --> ingestion
    s18 --> media
    s18 --> repos
    s19 --> ingestion
    s19 --> integrations
    s19 --> repos
    s20 --> ingestion
    s20 --> repos
    s21 --> ingestion
    s21 --> integrations
    s21 --> repos
    s22 --> integrations
    s22 --> media
    s22 --> repos
    s23 --> ingestion
    s23 --> repos
    s24 --> repos
    s26 --> ingestion
    s26 --> integrations
    s26 --> repos
    s28 --> ingestion
    s28 --> media
    s28 --> repos
    s29 --> ingestion
    s29 --> repos
    s31 --> ingestion
    s31 --> repos
    s32 --> integrations
    s32 --> media
    s32 --> repos
    s33 --> integrations
    s33 --> media
    s33 --> repos
```
