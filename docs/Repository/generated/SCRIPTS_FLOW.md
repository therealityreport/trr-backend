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
    subgraph sg30["scripts/1-ShowInfo"]
        s30["showinfo_step1"]
    end
    subgraph sg31["scripts/2-CastInfo"]
        s31["CastInfo_ArchiveStep"]
        s32["CastInfo_Step1"]
        s33["CastInfo_Step2"]
    end
    subgraph sg34["scripts/3-RealiteaseInfo"]
        s34["RealiteaseInfo_BackfillTMDb"]
        s35["RealiteaseInfo_Step1"]
        s36["RealiteaseInfo_Step2"]
        s37["RealiteaseInfo_Step3"]
        s38["RealiteaseInfo_Step4"]
        s39["RealiteaseInfo_archive"]
        s40["realiteaseinfo_birthdays_archive"]
        s41["ultimate_reality_tv_scraper"]
    end
    subgraph sg42["scripts/4-WWHLInfo"]
        s42["WWHLInfo_Checker_Step4"]
        s43["WWHLInfo_Gemini_Step3"]
        s44["WWHLInfo_IMDb_Step2"]
        s45["WWHLInfo_TMDb_Step1"]
    end
    subgraph sg46["scripts/5-FinalList"]
        s46["FinalInfo_Step1"]
        s47["FinalInfo_Step2"]
        s48["FinalInfo_Step3"]
        s49["FinalList_Builder"]
        s50["Firebase_Uploader"]
        s51["__init__"]
        s52["__main__"]
        s53["__main__"]
        s54["__main__"]
        s55["__main__"]
        s56["__main__"]
        s57["__main__"]
        s58["_cmd"]
        s59["_in_process"]
        s60["_lint_dependency_groups"]
        s61["_log_render"]
        s62["_musllinux"]
        s63["_pip_wrapper"]
        s64["_ratio"]
        s65["_win32_console"]
        s66["_windows"]
        s67["_wrap"]
        s68["abc"]
        s69["align"]
        s70["box"]
        s71["cells"]
        s72["certs"]
        s73["color"]
        s74["columns"]
        s75["console"]
        s76["control"]
        s77["default_styles"]
        s78["diagnose"]
        s79["distro"]
        s80["emoji"]
        s81["help"]
        s82["highlighter"]
        s83["json"]
        s84["layout"]
        s85["live"]
        s86["logging"]
        s87["markup"]
        s88["padding"]
        s89["pager"]
        s90["palette"]
        s91["panel"]
        s92["pretty"]
        s93["progress"]
        s94["progress_bar"]
        s95["prompt"]
        s96["repr"]
        s97["rule"]
        s98["scope"]
        s99["scripts"]
        s100["segment"]
        s101["spinner"]
        s102["status"]
        s103["styled"]
        s104["syntax"]
        s105["table"]
        s106["text"]
        s107["theme"]
        s108["traceback"]
        s109["tree"]
        s110["unistring"]
        s111["verify_finallist_snapshot"]
        s112["wheel"]
    end
    subgraph sg113["scripts/archives"]
        s113["add_tmdb_cast_ids"]
        s114["add_tmdb_ids_batch"]
        s115["build_realitease_info"]
        s116["enhance_realitease_bio_data"]
        s117["enhance_realitease_famous_birthdays"]
        s118["fetch_WWHL_info"]
        s119["fetch_WWHL_info_clean"]
        s120["fetch_WWHL_info_imdb_api"]
        s121["fetch_WWHL_info_imdb_chatgpt"]
        s122["fetch_WWHL_info_imdb_fast"]
        s123["fetch_missing_person_info"]
        s124["find_missing_cast_selective"]
        s125["smart_cast_filter"]
        s126["test_new_structure"]
        s127["test_update"]
        s128["tmdb_api_test_no_key"]
        s129["tmdb_corrected_extractor"]
        s130["tmdb_credit_id_test"]
        s131["tmdb_episode_details"]
        s132["tmdb_extractor_v6"]
        s133["tmdb_final_extractor"]
        s134["tmdb_focused_extractor"]
        s135["tmdb_imdb_conversion_test"]
        s136["tmdb_other_shows_extractor"]
        s137["tmdb_quick_test"]
        s138["tmdb_rupaul_extractor"]
        s139["tmdb_season_extractor_test"]
        s140["tmdb_simple_test"]
    end
    subgraph sg141["scripts/supabase"]
        s141["generate_schema_docs"]
    end
    subgraph trr["trr_backend/"]
        repos["repositories"]
        integrations["integrations"]
        ingestion["ingestion"]
        media["media"]
    end
    s0 --> repositories
    s0 --> integrations
    s0 --> ingestion
    s1 --> integrations
    s1 --> ingestion
    s1 --> repositories
    s1 --> media
    s1 --> repositories
    s1 --> integrations
    s1 --> repositories
    s3 --> integrations
    s4 --> repositories
    s4 --> integrations
    s4 --> repositories
    s5 --> repositories
    s5 --> integrations
    s5 --> ingestion
    s5 --> repositories
    s5 --> integrations
    s5 --> repositories
    s5 --> repositories
    s6 --> integrations
    s6 --> ingestion
    s7 --> repositories
    s7 --> media
    s7 --> repositories
    s8 --> repositories
    s8 --> media
    s9 --> media
    s10 --> repositories
    s10 --> integrations
    s10 --> ingestion
    s11 --> integrations
    s15 --> repositories
    s15 --> ingestion
    s15 --> repositories
    s15 --> media
    s15 --> repositories
    s16 --> repositories
    s16 --> repositories
    s16 --> integrations
    s16 --> ingestion
    s16 --> repositories
    s16 --> integrations
    s16 --> repositories
    s16 --> repositories
    s17 --> repositories
    s17 --> ingestion
    s18 --> repositories
    s18 --> repositories
    s18 --> integrations
    s18 --> ingestion
    s18 --> repositories
    s19 --> media
    s19 --> integrations
    s19 --> repositories
    s20 --> repositories
    s20 --> ingestion
    s21 --> repositories
    s22 --> repositories
    s22 --> repositories
    s22 --> integrations
    s22 --> ingestion
    s22 --> repositories
    s23 --> media
    s23 --> ingestion
    s23 --> repositories
    s24 --> repositories
    s24 --> repositories
    s24 --> ingestion
    s26 --> repositories
    s26 --> repositories
    s26 --> repositories
    s26 --> ingestion
    s27 --> repositories
    s27 --> repositories
    s27 --> media
    s27 --> integrations
    s28 --> media
    s28 --> integrations
    s28 --> repositories
```
