# Scripts Flow

Script entrypoints (`if __name__ == '__main__'`) and their trr_backend dependencies.

```mermaid
flowchart LR
    subgraph sg0["scripts (root)"]
        s0["backfill_tmdb_show_details"]
        s1["enrich_show_cast"]
        s2["fix_repo_structure_mermaid"]
        s3["generate_repo_mermaid"]
        s4["imdb_show_enrichment"]
        s5["import_fandom_gallery_photos"]
        s6["import_imdb_cast_episode_appearances"]
        s7["import_shows_from_lists"]
        s8["mirror_cast_photos_to_s3"]
        s9["mirror_show_images_to_s3"]
        s10["rebuild_hosted_urls"]
        s11["resolve_tmdb_ids_via_find"]
        s12["rhoslc_fandom_enrichment"]
        s13["run_pipeline"]
        s14["run_show_import_job"]
        s15["sync_all_tables"]
        s16["sync_cast_photos"]
        s17["sync_episode_appearances"]
        s18["sync_episodes"]
        s19["sync_people"]
        s20["sync_season_episode_images"]
        s21["sync_seasons"]
        s22["sync_seasons_episodes"]
        s23["sync_show_cast"]
        s24["sync_show_images"]
        s25["sync_shows"]
        s26["sync_shows_all"]
        s27["sync_tmdb_person_images"]
        s28["sync_tmdb_show_entities"]
        s29["sync_tmdb_watch_providers"]
        s30["verify_schema"]
    end
    subgraph sg1["scripts/supabase"]
        s31["generate_schema_docs"]
    end
    subgraph trr["trr_backend/"]
        ingestion["ingestion"]
        integrations["integrations"]
        media["media"]
        repos["repositories"]
    end
    s0 --> ingestion
    s0 --> integrations
    s0 --> repos
    s1 --> ingestion
    s1 --> integrations
    s1 --> media
    s1 --> repos
    s4 --> integrations
    s5 --> integrations
    s5 --> repos
    s6 --> ingestion
    s6 --> integrations
    s6 --> repos
    s7 --> ingestion
    s7 --> integrations
    s8 --> media
    s8 --> repos
    s9 --> media
    s9 --> repos
    s10 --> media
    s11 --> ingestion
    s11 --> integrations
    s11 --> repos
    s12 --> integrations
    s16 --> ingestion
    s16 --> media
    s16 --> repos
    s17 --> ingestion
    s17 --> integrations
    s17 --> repos
    s18 --> ingestion
    s18 --> repos
    s19 --> ingestion
    s19 --> integrations
    s19 --> repos
    s20 --> integrations
    s20 --> media
    s20 --> repos
    s21 --> ingestion
    s21 --> repos
    s22 --> repos
    s23 --> ingestion
    s23 --> integrations
    s23 --> repos
    s24 --> ingestion
    s24 --> media
    s24 --> repos
    s25 --> ingestion
    s25 --> repos
    s27 --> ingestion
    s27 --> repos
    s28 --> integrations
    s28 --> media
    s28 --> repos
    s29 --> integrations
    s29 --> media
    s29 --> repos
```
