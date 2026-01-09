# Mermaid Diagram

```mermaid
graph TD
    %% Repository Structure
    subgraph trr_backend
        direction TB
        root[trr-backend]
        root --> env[.env.example]
        root --> github[.github]
        root --> gitignore[.gitignore]
        root --> python_version[.python-version]
        root --> makefile[Makefile]
        root --> readme[README.md]
        root --> repo_structure[REPO_STRUCTURE.md]
        root --> backfill_tmdb[backfill_tmdb_show_details.py]
        root --> pytest[pytest.ini]
        root --> requirements[requirements.txt]
        root --> resolve_tmdb[resolve_tmdb_ids_via_find.py]
        root --> scripts[Scripts]
        root --> skills[Skills]
        root --> supabase[Supabase]
        root --> test_connection[test_connection.py]
        root --> tests[Tests]
        root --> trr_backend_dir[trr_backend]

        subgraph github_workflows[".github/workflows" ]
            direction TB
            github --> ci[ci.yml]
            github --> repo_map[repo_map.yml]
        end

        subgraph api["API"]
            direction TB
            root --> api
            api --> init[__init__.py]
            api --> auth[auth.py]
            api --> deps[deps.py]
            api --> main[main.py]
            api --> realtime[Realtime]
            api --> routers[Routers]

            subgraph realtime["realtime"]
                realtime --> init_r[__init__.py]
                realtime --> broker[broker.py]
                realtime --> events[events.py]
            end

            subgraph routers["routers"]
                routers --> init_ro[__init__.py]
                routers --> discussions[discussions.py]
                routers --> dms[dms.py]
                routers --> shows[shows.py]
                routers --> surveys[surveys.py]
                routers --> ws[ws.py]
            end
        end

        subgraph docs["Docs"]
            direction TB
            root --> docs
            docs --> HISTORY_PURGE[HISTORY_PURGE.md]
            docs --> README_local[README_local.md]
            docs --> repository[Repository]
            docs --> SHEET_EDIT_MAPPING[SHEET_EDIT_MAPPING.md]
            docs --> api_docs[API]
            docs --> architecture[Architecture]
            docs --> cloud[Cloud]
            docs --> db[Database]
            docs --> images[Images]
            docs --> runbooks[Runbooks]

            subgraph repository_structure["repository"]
                direction TB
                repository --> init_repo[README.md]
                repository --> diagrams[Diagrams]
                repository --> generated[Generated]

                subgraph diagrams["diagrams"]
                    diagrams --> git_workflow[git_workflow.md]
                    diagrams --> system_maps[system_maps.md]
                end

                subgraph generated["generated"]
                    generated --> gitkeep[.gitkeep]
                    generated --> CODE_IMPORT_GRAPH[CODE_IMPORT_GRAPH.md]
                    generated --> SCRIPTS_FLOW[SCRIPTS_FLOW.md]
                end
            end

            subgraph api_docs["api"]
                direction TB
                api_docs --> run[run.md]
            end

            subgraph architecture["architecture"]
                architecture --> integrations[integrations.md]
            end

            subgraph cloud["cloud"]
                cloud --> cloud_setup[cloud_setup.md]
                cloud --> quick_cloud_setup[quick_cloud_setup.md]
                cloud --> setup_codespaces[setup_codespaces_credentials.md]
            end

            subgraph db["db"]
                db --> commands[commands.md]
                db --> schema[schema.md]
                db --> verification[verification.md]
            end

            subgraph images["images"]
                images --> debug[debug_imdb_credits.png]
            end

            subgraph runbooks["runbooks"]
                runbooks --> show_import[show_import_job.md]
            end
        end

        subgraph scripts_diagram["Scripts"]
            direction TB
            root --> scripts
            scripts --> showinfo[1-ShowInfo]
            scripts --> castinfo[2-CastInfo]
            scripts --> realitease[3-RealiteaseInfo]
            scripts --> wwhlinfo[4-WWHLInfo]
            scripts --> final_list[5-FinalList]
            scripts --> media[Media]
            scripts --> realitease_info[RealiteaseInfo]
            scripts --> sync_common[_sync_common.py]
            scripts --> archives[Archives]
            scripts --> db_scripts[db]
            scripts --> enrich_show_cast[enrich_show_cast.py]
            scripts --> generate_repo[generate_repo_mermaid.py]
            scripts --> imdb_show[imdb_show_enrichment.py]
            scripts --> import_fandom[import_fandom_gallery_photos.py]
            scripts --> import_imdb[import_imdb_cast_episode_appearances.py]
            scripts --> import_shows[import_shows_from_lists.py]
            scripts --> mirror_cast[mirror_cast_photos_to_s3.py]
            scripts --> mirror_show[mirror_show_images_to_s3.py]
            scripts --> rebuild_hosted[rebuild_hosted_urls.py]
            scripts --> rhoslc[rhoslc_fandom_enrichment.py]
            scripts --> run_pipeline[run_pipeline.py]
            scripts --> run_show_import[run_show_import_job.py]
            scripts --> sync_all[sync_all_tables.py]
            scripts --> sync_cast[sync_cast_photos.py]
            scripts --> sync_episode[sync_episode_appearances.py]
            scripts --> sync_episodes[sync_episodes.py]
            scripts --> sync_people[sync_people.py]
            scripts --> sync_season[sync_season_episode_images.py]
            scripts --> sync_seasons[sync_seasons.py]
            scripts --> sync_seasons_episodes[sync_seasons_episodes.py]
            scripts --> sync_show[sync_show_cast.py]
            scripts --> sync_show_images[sync_show_images.py]
            scripts --> sync_shows[sync_shows.py]
            scripts --> sync_shows_all[sync_shows_all.py]
            scripts --> sync_tmdb_person[sync_tmdb_person_images.py]
            scripts --> sync_tmdb_show[sync_tmdb_show_entities.py]
            scripts --> sync_tmdb_watch[sync_tmdb_watch_providers.py]
            scripts --> verify_schema[verify_schema.py]

            subgraph ShowInfo[1-ShowInfo]
                direction TB
                showinfo --> readme_showinfo[README.md]
                showinfo --> step1[showinfo_step1.py]
            end
        
            subgraph CastInfo[2-CastInfo]
                direction TB
                castinfo --> CastInfo_ArchiveStep[CastInfo_ArchiveStep.py]
                castinfo --> CastInfo_Step1[CastInfo_Step1.py]
                castinfo --> CastInfo_Step2[CastInfo_Step2.py]
                castinfo --> readme_castinfo[README.md]
            end

            subgraph RealiteaseInfo[3-RealiteaseInfo]
                direction TB
                realitease --> readme_realitease[README.md]
                realitease --> RealiteaseInfo_BackfillTMDb[RealiteaseInfo_BackfillTMDb.py]
                realitease --> RealiteaseInfo_Step1[RealiteaseInfo_Step1.py]
                realitease --> RealiteaseInfo_Step2[RealiteaseInfo_Step2.py]
                realitease --> RealiteaseInfo_Step3[RealiteaseInfo_Step3.py]
                realitease --> RealiteaseInfo_Step4[RealiteaseInfo_Step4.py]
                realitease --> RealiteaseInfo_archive[RealiteaseInfo_archive.py]
                realitease --> realiteaseinfo_birthdays[realiteaseinfo_birthdays_archive.py]
                realitease --> ultimate[ultimate_reality_tv_scraper.py]
            end

            subgraph WWHLInfo[4-WWHLInfo]
                direction TB
                wwhlinfo --> readme_wwhl[README.md]
                wwhlinfo --> WWHLInfo_Checker_Step4[WWHLInfo_Checker_Step4.py]
                wwhlinfo --> WWHLInfo_Gemini_Step3[WWHLInfo_Gemini_Step3.py]
                wwhlinfo --> WWHLInfo_IMDb_Step2[WWHLInfo_IMDb_Step2.py]
                wwhlinfo --> WWHLInfo_TMDb_Step1[WWHLInfo_TMDb_Step1.py]
            end

            subgraph FinalList[5-FinalList]
                direction TB
                final_list --> FinalInfo_Step1[FinalInfo_Step1.py]
                final_list --> FinalInfo_Step2[FinalInfo_Step2.py]
                final_list --> FinalInfo_Step3[FinalInfo_Step3.py]
                final_list --> FinalList_Builder[FinalList_Builder.py]
                final_list --> Firebase_Uploader[Firebase_Uploader.py]
                final_list --> verify_finallist_snapshot[verify_finallist_snapshot.py]
            end
            
            subgraph db_scripts[db]
                direction TB
                db_scripts --> readme_db[README.md]
                db_scripts --> guard_core_schema[guard_core_schema.sql]
                db_scripts --> reload_postgrest_schema[reload_postgrest_schema.sql]
                db_scripts --> run_sql[run_sql.sh]
                db_scripts --> verify_pre_0033_cleanup[verify_pre_0033_cleanup.sql]
            end
            
            subgraph archives[Archives]
                direction TB
                archives --> add_tmdb_cast_ids[add_tmdb_cast_ids.py]
                archives --> add_tmdb_ids_batch[add_tmdb_ids_batch.py]
                archives --> build_realitease_info[build_realitease_info.py]
                archives --> enhance_realitease_bio_data[enhance_realitease_bio_data.py]
                archives --> enhance_realitease_famous_birthdays[enhance_realitease_famous_birthdays.py]
                archives --> fetch_WWHL_info[fetch_WWHL_info.py]
                archives --> fetch_WWHL_info_clean[fetch_WWHL_info_clean.py]
                archives --> fetch_WWHL_info_imdb_api[fetch_WWHL_info_imdb_api.py]
                archives --> fetch_WWHL_info_imdb_chatgpt[fetch_WWHL_info_imdb_chatgpt.py]
                archives --> fetch_WWHL_info_imdb_fast[fetch_WWHL_info_imdb_fast.py]
                archives --> fetch_missing_person_info[fetch_missing_person_info.py]
                archives --> fetch_person_details[fetch_person_details.py]
                archives --> find_missing_cast_selective[find_missing_cast_selective.py]
                archives --> smart_cast_filter[smart_cast_filter.py]
                archives --> test_new_structure[test_new_structure.py]
                archives --> test_update[test_update.py]
                archives --> tmdb_api_test_no_key[tmdb_api_test_no_key.py]
                archives --> tmdb_corrected_extractor[tmdb_corrected_extractor.py]
                archives --> tmdb_credit_id_test[tmdb_credit_id_test.py]
                archives --> tmdb_episode_details[tmdb_episode_details.py]
                archives --> tmdb_extractor_v6[tmdb_extractor_v6.py]
                archives --> tmdb_final_extractor[tmdb_final_extractor.py]
                archives --> tmdb_focused_extractor[tmdb_focused_extractor.py]
                archives --> tmdb_imdb_conversion_test[tmdb_imdb_conversion_test.py]
                archives --> tmdb_other_shows_extractor[tmdb_other_shows_extractor.py]
                archives --> tmdb_quick_test[tmdb_quick_test.py]
                archives --> tmdb_rupaul_extractor[tmdb_rupaul_extractor.py]
                archives --> tmdb_season_extractor_test[tmdb_season_extractor_test.py]
                archives --> tmdb_simple_test[tmdb_simple_test.py]
            end

        end

        subgraph skills_diagram["Skills"]
            direction TB
            skills --> database_designer[database-designer]
            database_designer --> skill[SKILL.md]
            database_designer --> references[References]
            
            subgraph references["references"]
                direction TB
                references --> examples[examples.md]
                references --> playbooks[playbooks.md]
                references --> repo_context[repo-context.md]
                references --> templates[templates.sql]
                references --> tooling[tooling.md]
            end
        end

        subgraph supabase_diagram["Supabase"]
            direction TB
            supabase --> gitignore_supabase[.gitignore]
            supabase --> config[config.toml]
            supabase --> migrations[Migrations]
            supabase --> schema_docs[Schema Docs]
            supabase --> seed[seed.sql]

            subgraph migrations["Migrations"]
                direction TB
                migrations --> sql1[0001_init.sql]
                migrations --> sql2[0002_social.sql]
                migrations --> sql3[0003_dms.sql]
                migrations --> sql4[0004_core_shows.sql]
                migrations --> sql5[0005_show_images.sql]
                migrations --> sql6[0006_show_images_grants.sql]
                migrations --> sql7[0007_core_shows_tmdb_id.sql]
                migrations --> sql8[0008_show_images_tmdb_id.sql]
                migrations --> sql9[0009_show_images_view.sql]
                migrations --> sql10[0010_show_images_no_votes.sql]
                migrations --> sql11[0011_show_images_view_no_votes.sql]
                migrations --> sql12[0012_seasons_and_episodes.sql]
                migrations --> sql13[0013_season_images.sql]
                migrations --> sql14[0014_show_seasons_view.sql]
                migrations --> sql15[0015_seasons_show_name.sql]
                migrations --> sql16[0016_seasons_episode_id_arrays.sql]
                migrations --> sql17[0017_episodes_show_name.sql]
                migrations --> sql18[0018_imdb_cast_episode_appearances.sql]
                migrations --> sql19[0019_imdb_cast_grants.sql]
                migrations --> sql20[0020_reorder_show_tables.sql]
                migrations --> sql21[0021_reorder_people_cast_seasons_episodes.sql]
                migrations --> sql22[0022_episode_appearances_export_view.sql]
                migrations --> sql23[0023_episode_appearances_export_view_total_episodes.sql]
                migrations --> sql24[0024_episode_appearances_aggregate.sql]
                migrations --> sql25[0025_sync_state.sql]
                migrations --> sql26[0026_add_imdb_meta_to_core_shows.sql]
                migrations --> sql27[0027_show_images_media_sources.sql]
                migrations --> sql28[0028_normalize_shows_add_columns.sql]
                migrations --> sql29[0029_create_source_tables.sql]
                migrations --> sql30[0030_create_normalized_child_tables.sql]
                migrations --> sql31[0031_update_show_images_typed.sql]
                migrations --> sql32[0032_backfill_normalized_data.sql]
                migrations --> sql33[0033_cleanup_legacy_jsonb_columns.sql]
                migrations --> sql34[0034_show_images_constraints_and_show_flags.sql]
                migrations --> sql35[0035_show_images_upsert_rpc.sql]
                migrations --> sql36[0036_show_merge_helpers.sql]
                migrations --> sql37[0037_collapse_show_attributes.sql]
                migrations --> sql38[0038_update_merge_shows_arrays.sql]
                migrations --> sql39[0039_drop_child_tables.sql]
                migrations --> sql40[0040_create_cast_photos.sql]
                migrations --> sql41[0041_create_cast_fandom_and_extend_cast_photos.sql]
                migrations --> sql42[0042_revoke_cast_public_access.sql]
                migrations --> sql43[0043_cast_photos_add_hosted_fields.sql]
                migrations --> sql44[0044_create_cast_tmdb.sql]
                migrations --> sql45[0045_show_images_add_hosted_fields.sql]
                migrations --> sql46[0046_cast_photos_allow_tmdb_source.sql]
                migrations --> sql47[0047_add_show_source_metadata.sql]
                migrations --> sql48[0048_create_tmdb_entities_and_watch_providers.sql]
                migrations --> sql49[0049_rename_tmdb_dimension_tables.sql]
                migrations --> sql50[0050_drop_or_view_tmdb_imdb_series.sql]
                migrations --> sql51[0051_season_images_add_hosted_fields.sql]
            end
            
            subgraph schema_docs["Schema Docs"]
                direction TB
                schema_docs --> INDEX[INDEX.md]
                schema_docs --> core_cast_memberships[core.cast_memberships.json]
                schema_docs --> core_cast_memberships_md[core.cast_memberships.md]
                schema_docs --> core_episode_appearances[core.episode_appearances.json]
                schema_docs --> core_episode_appearances_md[core.episode_appearances.md]
                schema_docs --> core_episode_cast[core.episode_cast.json]
                schema_docs --> core_episode_cast_md[core.episode_cast.md]
                schema_docs --> core_episodes[core.episodes.json]
                schema_docs --> core_episodes_md[core.episodes.md]
                schema_docs --> core_people[core.people.json]
                schema_docs --> core_people_md[core.people.md]
                schema_docs --> core_season_images[core.season_images.json]
                schema_docs --> core_season_images_md[core.season_images.md]
                schema_docs --> core_seasons[core.seasons.json]
                schema_docs --> core_seasons_md[core.seasons.md]
                schema_docs --> core_show_cast[core.show_cast.json]
                schema_docs --> core_show_cast_md[core.show_cast.md]
                schema_docs --> core_show_images[core.show_images.json]
                schema_docs --> core_show_images_md[core.show_images.md]
                schema_docs --> core_shows[core.shows.json]
                schema_docs --> core_shows_md[core.shows.md]
                schema_docs --> core_sync_state[core.sync_state.json]
                schema_docs --> core_sync_state_md[core.sync_state.md]
            end
        end

        subgraph tests_diagram["Tests"]
            direction TB
            tests --> init_t[__init__.py]
            tests --> fixtures[Fixtures]
            tests --> ingestion[Ingestion]
            tests --> integrations[Integrations]
            tests --> media[Media]
            tests --> migrations[Migrations]
            tests --> repositories[Repositories]
            tests --> scripts[Scripts]
            tests --> test_api[test_api_smoke.py]
            tests --> test_discussions[test_discussions_smoke.py]
            tests --> test_dms[test_dms_smoke.py]
            tests --> test_ws[test_ws_realtime_smoke.py]
            tests --> utils[Utils]

            subgraph fixtures["Fixtures"]
                direction TB
                fixtures --> fandom[Fandom]
                fixtures --> imdb[IMDB]
                fixtures --> tmdb[TMDB]

                subgraph fandom["Fandom"]
                    direction TB
                    fandom --> lisa_barlow_infobox[lisa_barlow_infobox.html]
                    fandom --> lisa_barlow_person_sample[lisa_barlow_person_sample.html]
                end

                subgraph imdb["IMDB"]
                    direction TB
                    imdb --> episodes_page_overview_one_season[episodes_page_overview_one_season_sample.html]
                    imdb --> episodes_page_overview[episodes_page_overview_sample.html]
                    imdb --> episodes_page_season1_next[episodes_page_season1_next_data_sample.html]
                    imdb --> episodes_page_season3[episodes_page_season3_sample.html]
                    imdb --> fullcredits_cast[fullcredits_cast_sample.html]
                    imdb --> list_html_fallback[list_html_fallback_sample.html]
                    imdb --> list_jsonld[list_jsonld_sample.html]
                    imdb --> list_sample[list_sample.html]
                    imdb --> list_sample_page2[list_sample_page2.html]
                    imdb --> mediaindex[mediaindex_tt8819906_sample.html]
                    imdb --> mediaindex_viewer[mediaindex_viewer_graphql_tt8819906_sample.html]
                    imdb --> person_mediaindex[person_mediaindex_nm11883948_sample.html]
                    imdb --> person_mediaviewer[person_mediaviewer_nm11883948_rm1679992066_sample.html]
                    imdb --> section_images[section_images_sample.html]
                    imdb --> title_list_main_page[title_list_main_page_sample.json]
                    imdb --> title_page[title_page_sample.html]
                    imdb --> title_page_tt[title_page_tt8819906_sample.html]
                end

                subgraph tmdb["TMDB"]
                    direction TB
                    tmdb --> find_by_imdb_id[find_by_imdb_id_sample.json]
                    tmdb --> tv_details_full[tv_details_full_sample.json]
                    tmdb --> tv_details[tv_details_sample.json]
                    tmdb --> tv_images[tv_images_sample.json]
                    tmdb --> tv_season_details[tv_season_details_sample.json]
                    tmdb --> tv_watch_providers[tv_watch_providers_sample.json]
                end
            end

            subgraph ingestion["Ingestion"]
                direction TB
                ingestion --> test_episode_appearances_upsert[test_episode_appearances_upsert.py]
                ingestion --> test_fandom_person_scraper[test_fandom_person_scraper.py]
                ingestion --> test_show_importer_metadata_enrichment[test_show_importer_metadata_enrichment.py]
                ingestion --> test_show_importer_tmdb_details_links_imdb_show[test_show_importer_tmdb_details_links_imdb_show.py]
                ingestion --> test_show_metadata_enricher[test_show_metadata_enricher.py]
                ingestion --> test_tmdb_show_backfill[test_tmdb_show_backfill.py]
            end

            subgraph integrations["Integrations"]
                direction TB
                integrations --> fandom_integration[Fandom]
                integrations --> imdb_integration[IMDB]
                integrations --> tmdb_integration[TMDB]

                subgraph fandom_integration["Fandom"]
                    direction TB
                    fandom_integration --> test_fandom_infobox_parser[test_fandom_infobox_parser.py]
                end

                subgraph imdb_integration["IMDB"]
                    direction TB
                    imdb_integration --> test_episodic_client_normalization[test_episodic_client_normalization.py]
                    imdb_integration --> test_fullcredits_cast_parser[test_fullcredits_cast_parser.py]
                    imdb_integration --> test_imdb_episodes_persistence[test_imdb_episodes_persistence.py]
                    imdb_integration --> test_imdb_images[test_imdb_images.py]
                    imdb_integration --> test_imdb_list_graphql_client_parsing[test_imdb_list_graphql_client_parsing.py]
                    imdb_integration --> test_mediaindex_images[test_mediaindex_images.py]
                    imdb_integration --> test_person_gallery_parser[test_person_gallery_parser.py]
                    imdb_integration --> test_title_page_metadata[test_title_page_metadata.py]
                end

                subgraph tmdb_integration["TMDB"]
                    direction TB
                    tmdb_integration --> test_tmdb_season_enrichment[test_tmdb_season_enrichment.py]
                    tmdb_integration --> test_tmdb_tv_details_persistence[test_tmdb_tv_details_persistence.py]
                    tmdb_integration --> test_tmdb_tv_images_persistence[test_tmdb_tv_images_persistence.py]
                end
            end

            subgraph media["Media"]
                direction TB
                media --> test_s3_mirror[test_s3_mirror.py]
            end

            subgraph migrations["Migrations"]
                direction TB
                migrations --> test_show_source_metadata_migrations[test_show_source_metadata_migrations.py]
            end

            subgraph repositories["Repositories"]
                direction TB
                repositories --> test_cast_photos_upsert[test_cast_photos_upsert.py]
                repositories --> test_pgrst204_retry[test_pgrst204_retry.py]
                repositories --> test_shows_preflight[test_shows_preflight.py]
            end

            subgraph scripts["Scripts"]
                direction TB
                scripts --> test_import_shows_from_lists_merge[test_import_shows_from_lists_merge.py]
                scripts --> test_import_shows_from_lists_parsing[test_import_shows_from_lists_parsing.py]
                scripts --> test_import_shows_from_lists_upsert[test_import_shows_from_lists_upsert.py]
                scripts --> test_sync_incremental[test_sync_incremental.py]
                scripts --> test_sync_tmdb_watch_providers[test_sync_tmdb_watch_providers.py]
            end

            subgraph utils["Utils"]
                direction TB
                utils --> test_episode_appearances_aggregation[test_episode_appearances_aggregation.py]
            end
        end

        subgraph trr_backend_dir["trr_backend"]
            direction TB
            trr_backend_dir --> init_tb[__init__.py]
            trr_backend_dir --> db[DB]
            trr_backend_dir --> ingestion[Ingestion]
            trr_backend_dir --> integrations[Integrations]
            trr_backend_dir --> media[Media]
            trr_backend_dir --> models[Models]
            trr_backend_dir --> repositories[Repositories]
            trr_backend_dir --> utils[Utils]

            subgraph db["DB"]
                direction TB
                db --> init_db[__init__.py]
                db --> connection[connection.py]
                db --> postgrest_cache[postgrest_cache.py]
                db --> preflight[preflight.py]
                db --> show_images[show_images.py]
                db --> supabase[supabase.py]
            end

            subgraph ingestion["Ingestion"]
                direction TB
                ingestion --> init_ing[__init__.py]
                ingestion --> cast_photo_sources[cast_photo_sources.py]
                ingestion --> fandom_person_scraper[fandom_person_scraper.py]
                ingestion --> imdb_images[imdb_images.py]
                ingestion --> show_importer[show_importer.py]
                ingestion --> show_metadata_enricher[show_metadata_enricher.py]
                ingestion --> showinfo_overrides[showinfo_overrides.py]
                ingestion --> shows_from_lists[shows_from_lists.py]
                ingestion --> tmdb_person_images[tmdb_person_images.py]
                ingestion --> tmdb_show_backfill[tmdb_show_backfill.py]
            end

            subgraph integrations["Integrations"]
                direction TB
                integrations --> init_int[__init__.py]
                integrations --> fandom[fandom.py]

                subgraph imdb["IMDB"]
                    direction TB
                    imdb --> init_imdb[__init__.py]
                    imdb --> credits_client[credits_client.py]
                    imdb --> episodic_client[episodic_client.py]
                    imdb --> fullcredits_cast_parser[fullcredits_cast_parser.py]
                    imdb --> list_graphql_client[list_graphql_client.py]
                    imdb --> mediaindex_images[mediaindex_images.py]
                    imdb --> person_gallery[person_gallery.py]
                    imdb --> title_metadata_client[title_metadata_client.py]
                    imdb --> title_page_metadata[title_page_metadata.py]
                end

                integrations --> tmdb[tmdb]
                tmdb --> init_tmdb[__init__.py]
                tmdb --> client[client.py]
                integrations --> tmdb_person[tmdb_person.py]
            end

            subgraph media["Media"]
                direction TB
                media --> init_m[__init__.py]
                media --> s3_mirror[s3_mirror.py]
            end

            subgraph models["Models"]
                direction TB
                models --> init_mo[__init__.py]
                models --> cast_photos[cast_photos.py]
                models --> shows[shows.py]
            end

            subgraph repositories["Repositories"]
                direction TB
                repositories --> init_re[__init__.py]
                repositories --> cast_fandom[cast_fandom.py]
                repositories --> cast_photos[cast_photos.py]
                repositories --> cast_tmdb[cast_tmdb.py]
                repositories --> episode_appearances[episode_appearances.py]
                repositories --> episodes[episodes.py]
                repositories --> imdb_series[imdb_series.py]
                repositories --> people[people.py]
                repositories --> season_images[season_images.py]
                repositories --> seasons[seasons.py]
                repositories --> show_cast[show_cast.py]
                repositories --> show_images[show_images.py]
                repositories --> shows[shows.py]
                repositories --> sync_state[sync_state.py]
                repositories --> tmdb_series[tmdb_series.py]
            end

            subgraph utils["Utils"]
                direction TB
                utils --> init_u[__init__.py]
                utils --> env[env.py]
                utils --> episode_appearances[episode_appearances.py]
            end
        end
    end
    style trr_backend fill:#e0f7fa,stroke:#333,stroke-width:2px;
    style scripts_diagram fill:#d0f0c0,stroke:#333,stroke-width:2px;
    style tests_diagram fill:#ffe6cc,stroke:#333,stroke-width:2px;
    style skills_diagram fill:#e6ccff,stroke:#333,stroke-width:2px;
    style supabase_diagram fill:#ffccff,stroke:#333,stroke-width:2px;
```