# Scripts Flow

Script entrypoints (`if __name__ == '__main__'`) and their trr_backend dependencies.

```mermaid
flowchart LR
    subgraph sg0["scripts (root)"]
        s0["backfill_tmdb_show_details"]
        s1["enrich_show_cast"]
        s2["generate_repo_mermaid"]
        s3["imdb_show_enrichment"]
        s4["import_fandom_gallery_photos"]
        s5["import_imdb_cast_episode_appearances"]
        s6["import_shows_from_lists"]
        s7["mirror_cast_photos_to_s3"]
        s8["mirror_show_images_to_s3"]
        s9["rebuild_hosted_urls"]
        s10["resolve_tmdb_ids_via_find"]
        s11["rhoslc_fandom_enrichment"]
        s12["run_pipeline"]
        s13["run_show_import_job"]
        s14["sync_all_tables"]
        s15["sync_cast_photos"]
        s16["sync_episode_appearances"]
        s17["sync_episodes"]
        s18["sync_people"]
        s19["sync_season_episode_images"]
        s20["sync_seasons"]
        s21["sync_seasons_episodes"]
        s22["sync_show_cast"]
        s23["sync_show_images"]
        s24["sync_shows"]
        s25["sync_shows_all"]
        s26["sync_tmdb_person_images"]
        s27["sync_tmdb_show_entities"]
        s28["sync_tmdb_watch_providers"]
        s29["verify_schema"]
    end
    subgraph sg1["scripts/1-ShowInfo"]
        s30["showinfo_step1"]
    end
    subgraph sg2["scripts/2-CastInfo"]
        s31["CastInfo_ArchiveStep"]
        s32["CastInfo_Step1"]
        s33["CastInfo_Step2"]
    end
    subgraph sg3["scripts/3-RealiteaseInfo"]
        s34["RealiteaseInfo_BackfillTMDb"]
        s35["RealiteaseInfo_Step1"]
        s36["RealiteaseInfo_Step2"]
        s37["RealiteaseInfo_Step3"]
        s38["RealiteaseInfo_Step4"]
        s39["RealiteaseInfo_archive"]
        s40["realiteaseinfo_birthdays_archive"]
        s41["ultimate_reality_tv_scraper"]
    end
    subgraph sg4["scripts/4-WWHLInfo"]
        s42["WWHLInfo_Checker_Step4"]
        s43["WWHLInfo_Gemini_Step3"]
        s44["WWHLInfo_IMDb_Step2"]
        s45["WWHLInfo_TMDb_Step1"]
    end
    subgraph sg5["scripts/5-FinalList"]
        s46["FinalInfo_Step1"]
        s47["FinalInfo_Step2"]
        s48["FinalInfo_Step3"]
        s49["FinalList_Builder"]
        s50["Firebase_Uploader"]
        s51["verify_finallist_snapshot"]
    end
    subgraph sg6["scripts/archives"]
        s52["add_tmdb_cast_ids"]
        s53["add_tmdb_ids_batch"]
        s54["build_realitease_info"]
        s55["enhance_realitease_bio_data"]
        s56["enhance_realitease_famous_birthdays"]
        s57["fetch_WWHL_info"]
        s58["fetch_WWHL_info_clean"]
        s59["fetch_WWHL_info_imdb_api"]
        s60["fetch_WWHL_info_imdb_chatgpt"]
        s61["fetch_WWHL_info_imdb_fast"]
        s62["fetch_missing_person_info"]
        s63["find_missing_cast_selective"]
        s64["smart_cast_filter"]
        s65["test_new_structure"]
        s66["test_update"]
        s67["tmdb_api_test_no_key"]
        s68["tmdb_corrected_extractor"]
        s69["tmdb_credit_id_test"]
        s70["tmdb_episode_details"]
        s71["tmdb_extractor_v6"]
        s72["tmdb_final_extractor"]
        s73["tmdb_focused_extractor"]
        s74["tmdb_imdb_conversion_test"]
        s75["tmdb_other_shows_extractor"]
        s76["tmdb_quick_test"]
        s77["tmdb_rupaul_extractor"]
        s78["tmdb_season_extractor_test"]
        s79["tmdb_simple_test"]
    end
    subgraph sg7["scripts/supabase"]
        s80["generate_schema_docs"]
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
    s3 --> integrations
    s4 --> integrations
    s4 --> repos
    s5 --> ingestion
    s5 --> integrations
    s5 --> repos
    s6 --> ingestion
    s6 --> integrations
    s7 --> media
    s7 --> repos
    s8 --> media
    s8 --> repos
    s9 --> media
    s10 --> ingestion
    s10 --> integrations
    s10 --> repos
    s11 --> integrations
    s15 --> ingestion
    s15 --> media
    s15 --> repos
    s16 --> ingestion
    s16 --> integrations
    s16 --> repos
    s17 --> ingestion
    s17 --> repos
    s18 --> ingestion
    s18 --> integrations
    s18 --> repos
    s19 --> integrations
    s19 --> media
    s19 --> repos
    s20 --> ingestion
    s20 --> repos
    s21 --> repos
    s22 --> ingestion
    s22 --> integrations
    s22 --> repos
    s23 --> ingestion
    s23 --> media
    s23 --> repos
    s24 --> ingestion
    s24 --> repos
    s26 --> ingestion
    s26 --> repos
    s27 --> integrations
    s27 --> media
    s27 --> repos
    s28 --> integrations
    s28 --> media
    s28 --> repos
```
