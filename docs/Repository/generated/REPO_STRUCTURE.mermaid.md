# Mermaid Diagram

```mermaid
graph TD
    %% Repository Structure
    subgraph trr_backend
        direction TB
        root[trr-backend]
        
        %% Root Files
        root --> env[.env.example]
        root --> gitignore[.gitignore]
        root --> python_version[.python-version]
        root --> makefile[Makefile]
        root --> readme[README.md]
        root --> repo_structure[REPO_STRUCTURE.md]
        root --> backfill[backfill_tmdb_show_details.py]
        root --> pytest[pytest.ini]
        root --> requirements[requirements.txt]
        root --> resolve[resolve_tmdb_ids_via_find.py]
        root --> test_connection[test_connection.py]
        
        %% .github Folder
        subgraph github[".github"]
            workflows["workflows"]
            github --> workflows
            workflows --> ci[ci.yml]
            workflows --> repo_map[repo_map.yml]
        end

        %% api Folder
        subgraph api_folder["api"]
            direction TB
            api_folder --> init[__init__.py]
            api_folder --> auth[auth.py]
            api_folder --> deps[deps.py]
            api_folder --> main[main.py]
            subgraph realtime_folder["realtime"]
                realtime_folder --> realtime_init[__init__.py]
                realtime_folder --> broker[broker.py]
                realtime_folder --> events[events.py]
            end
            api_folder --> realtime_folder
            subgraph routers_folder["routers"]
                routers_folder --> routers_init[__init__.py]
                routers_folder --> discussions[discussions.py]
                routers_folder --> dms[dms.py]
                routers_folder --> shows[shows.py]
                routers_folder --> surveys[surveys.py]
                routers_folder --> ws[ws.py]
            end
            api_folder --> routers_folder
        end
        
        %% docs Folder
        subgraph docs_folder["docs"]
            direction TB
            docs_folder --> history_purge[HISTORY_PURGE.md]
            docs_folder --> readme_local[README_local.md]
            subgraph repository_folder["Repository"]
                repository_folder --> repo_readme[README.md]
                repository_folder --> diagrams[diagrams]
                diagrams --> git_workflow[git_workflow.md]
                diagrams --> system_maps[system_maps.md]
                repository_folder --> generated[generated]
                generated --> gitkeep[.gitkeep]
                generated --> code_import_graph[CODE_IMPORT_GRAPH.md]
                generated --> repo_structure_md[REPO_STRUCTURE.md]
                generated --> repo_structure_mermaid[REPO_STRUCTURE.mermaid.md]
                generated --> scripts_flow[SCRIPTS_FLOW.md]
            end
            docs_folder --> repository_folder
            docs_folder --> sheet_edit_mapping[SHEET_EDIT_MAPPING.md]
            docs_folder --> api_subfolder["api"]
            api_subfolder --> run["run.md"]
            docs_folder --> architecture[architecture.md]
            docs_folder --> cloud[cloud]
            cloud --> cloud_setup[cloud_setup.md]
            cloud --> quick_setup[quick_cloud_setup.md]
            cloud --> codespaces[setup_codespaces_credentials.md]
            docs_folder --> db[db]
            db --> commands[commands.md]
            db --> schema[schema.md]
            db --> verification[verification.md]
            docs_folder --> images[images]
            images --> debug_image[debug_imdb_credits.png]
            docs_folder --> runbooks[runbooks]
            runbooks --> show_import_job[show_import_job.md]
        end
        
        %% scripts Folder
        subgraph scripts_folder["scripts"]
            direction TB
            scripts_folder --> step1["1-ShowInfo"]
            step1 --> step1_readme[README.md]
            step1 --> showinfo_step1[showinfo_step1.py]
            scripts_folder --> step2["2-CastInfo"]
            step2 --> castinfo_archive[CastInfo_ArchiveStep.py]
            step2 --> castinfo_step1[CastInfo_Step1.py]
            step2 --> castinfo_step2[CastInfo_Step2.py]
            step2 --> step2_readme[README.md]
            scripts_folder --> step3["3-RealiteaseInfo"]
            step3 --> step3_readme[README.md]
            step3 --> step3_backfill[RealiteaseInfo_BackfillTMDb.py]
            step3 --> step3_step1[RealiteaseInfo_Step1.py]
            step3 --> step3_step2[RealiteaseInfo_Step2.py]
            step3 --> step3_step3[RealiteaseInfo_Step3.py]
            step3 --> step3_step4[RealiteaseInfo_Step4.py]
            step3 --> step3_archive[RealiteaseInfo_archive.py]
            step3 --> birthdays[realiteaseinfo_birthdays_archive.py]
            step3 --> scraper[ultimate_reality_tv_scraper.py]
            scripts_folder --> step4["4-WWHLInfo"]
            step4 --> step4_readme[README.md]
            step4 --> step4_step4[WWHLInfo_Checker_Step4.py]
            step4 --> step4_step3[WWHLInfo_Gemini_Step3.py]
            step4 --> step4_step2[WWHLInfo_IMDb_Step2.py]
            step4 --> step4_step1[WWHLInfo_TMDb_Step1.py]
            scripts_folder --> step5["5-FinalList"]
            step5 --> final_step1[FinalInfo_Step1.py]
            step5 --> final_step2[FinalInfo_Step2.py]
            step5 --> final_step3[FinalInfo_Step3.py]
            step5 --> final_list_builder[FinalList_Builder.py]
            step5 --> uploader[Firebase_Uploader.py]
            step5 --> verify[verify_finallist_snapshot.py]
            scripts_folder --> media["Media"]
            media --> media_readme[README.md]
            scripts_folder --> realitease["RealiteaseInfo"]
            realitease --> realitease_readme[README.md]
            scripts_folder --> sync_common[_sync_common.py]

            %% archives Folder
            subgraph archives_folder["archives"]
                direction TB
                archives_folder --> add_tmdb_cast_ids[add_tmdb_cast_ids.py]
                archives_folder --> add_tmdb_ids_batch[add_tmdb_ids_batch.py]
                archives_folder --> build_realitease_info[build_realitease_info.py]
                archives_folder --> enhance_bio[enhance_realitease_bio_data.py]
                archives_folder --> enhance_birthdays[enhance_realitease_famous_birthdays.py]
                archives_folder --> fetch_wwhl[fetch_WWHL_info.py]
                archives_folder --> fetch_wwhl_clean[fetch_WWHL_info_clean.py]
                archives_folder --> fetch_wwhl_imdb[fetch_WWHL_info_imdb_api.py]
                archives_folder --> fetch_wwhl_chatgpt[fetch_WWHL_info_imdb_chatgpt.py]
                archives_folder --> fetch_wwhl_fast[fetch_WWHL_info_imdb_fast.py]
                archives_folder --> fetch_missing_person[fetch_missing_person_info.py]
                archives_folder --> fetch_person[fetch_person_details.py]
                archives_folder --> find_missing_cast[find_missing_cast_selective.py]
                archives_folder --> smart_filter[smart_cast_filter.py]
                archives_folder --> test_new_structure[test_new_structure.py]
                archives_folder --> test_update[test_update.py]
                archives_folder --> tmdb_api_test[tmdb_api_test_no_key.py]
                archives_folder --> tmdb_corrected[tmdb_corrected_extractor.py]
                archives_folder --> tmdb_credit_id[tmdb_credit_id_test.py]
                archives_folder --> tmdb_episode_details[tmdb_episode_details.py]
                archives_folder --> tmdb_extractor_v6[tmdb_extractor_v6.py]
                archives_folder --> tmdb_final_extractor[tmdb_final_extractor.py]
                archives_folder --> tmdb_focused_extractor[tmdb_focused_extractor.py]
                archives_folder --> tmdb_imdb_conversion_test[tmdb_imdb_conversion_test.py]
                archives_folder --> tmdb_other_shows[tmdb_other_shows_extractor.py]
                archives_folder --> tmdb_quick_test[tmdb_quick_test.py]
                archives_folder --> tmdb_rupaul[tmdb_rupaul_extractor.py]
                archives_folder --> tmdb_season_extractor[tmdb_season_extractor_test.py]
                archives_folder --> tmdb_simple_test[tmdb_simple_test.py]
            end
            scripts_folder --> archives_folder
            
            %% db Folder in scripts
            subgraph db_folder["db"]
                direction TB
                db_folder --> db_readme[README.md]
                db_folder --> guard_core_schema[guard_core_schema.sql]
                db_folder --> reload_postgrest[reload_postgrest_schema.sql]
                db_folder --> run_sql[run_sql.sh]
                db_folder --> verify[verify_pre_0033_cleanup.sql]
            end
            scripts_folder --> db_folder
            
            %% supabase Folder in scripts
            subgraph supabase_folder["supabase"]
                direction TB
                supabase_folder --> generate_schema_docs[generate_schema_docs.py]
            end
            scripts_folder --> supabase_folder
            
            scripts_folder --> sync_all_tables[sync_all_tables.py]
            scripts_folder --> sync_cast_photos[sync_cast_photos.py]
            scripts_folder --> sync_episode_appearances[sync_episode_appearances.py]
            scripts_folder --> sync_episodes[sync_episodes.py]
            scripts_folder --> sync_people[sync_people.py]
            scripts_folder --> sync_season_images[sync_season_episode_images.py]
            scripts_folder --> sync_seasons[sync_seasons.py]
            scripts_folder --> sync_seasons_episodes[sync_seasons_episodes.py]
            scripts_folder --> sync_show_cast[sync_show_cast.py]
            scripts_folder --> sync_show_images[sync_show_images.py]
            scripts_folder --> sync_shows[sync_shows.py]
            scripts_folder --> sync_shows_all[sync_shows_all.py]
            scripts_folder --> sync_tmdb_person_images[sync_tmdb_person_images.py]
            scripts_folder --> sync_tmdb_show_entities[sync_tmdb_show_entities.py]
            scripts_folder --> sync_tmdb_watch_providers[sync_tmdb_watch_providers.py]
            scripts_folder --> verify_schema[verify_schema.py]
        end
        
        %% skills Folder
        subgraph skills_folder["skills"]
            direction TB
            skills_folder --> database_designer[database-designer]
            database_designer --> skill[SKILL.md]
            subgraph references_folder["references"]
                references_folder --> examples[examples.md]
                references_folder --> playbooks[playbooks.md]
                references_folder --> repo_context[repo-context.md]
                references_folder --> templates[templates.sql]
                references_folder --> tooling[tooling.md]
            end
            database_designer --> references_folder
        end
        
        %% supabase Folder
        subgraph supabase_folder["supabase"]
            direction TB
            supabase_folder --> supabase_gitignore[.gitignore]
            supabase_folder --> config[config.toml]
            subgraph migrations_folder["migrations"]
                direction TB
                migrations_folder --> init[0001_init.sql]
                migrations_folder --> social[0002_social.sql]
                migrations_folder --> dms[0003_dms.sql]
                migrations_folder --> core_shows[0004_core_shows.sql]
                migrations_folder --> show_images[0005_show_images.sql]
                migrations_folder --> show_images_grants[0006_show_images_grants.sql]
                migrations_folder --> core_shows_tmdb_id[0007_core_shows_tmdb_id.sql]
                migrations_folder --> show_images_tmdb_id[0008_show_images_tmdb_id.sql]
                migrations_folder --> show_images_view[0009_show_images_view.sql]
                migrations_folder --> show_images_no_votes[0010_show_images_no_votes.sql]
                migrations_folder --> show_images_view_no_votes[0011_show_images_view_no_votes.sql]
                migrations_folder --> seasons_and_episodes[0012_seasons_and_episodes.sql]
                migrations_folder --> season_images[0013_season_images.sql]
                migrations_folder --> seasons_show_name[0014_show_seasons_view.sql]
                migrations_folder --> seasons_episode_id_arrays[0016_seasons_episode_id_arrays.sql]
                migrations_folder --> episodes_show_name[0017_episodes_show_name.sql]
                migrations_folder --> imdb_cast_episode_appearances[0018_imdb_cast_episode_appearances.sql]
                migrations_folder --> imdb_cast_grants[0019_imdb_cast_grants.sql]
                migrations_folder --> reorder_show_tables[0020_reorder_show_tables.sql]
                migrations_folder --> reorder_people_cast_seasons_episodes[0021_reorder_people_cast_seasons_episodes.sql]
                migrations_folder --> episode_appearances_export_view[0022_episode_appearances_export_view.sql]
                migrations_folder --> episode_appearances_export_view_total[0023_episode_appearances_export_view_total_episodes.sql]
                migrations_folder --> episode_appearances_aggregate[0024_episode_appearances_aggregate.sql]
                migrations_folder --> sync_state[0025_sync_state.sql]
                migrations_folder --> add_imdb_meta[0026_add_imdb_meta_to_core_shows.sql]
                migrations_folder --> show_images_media_sources[0027_show_images_media_sources.sql]
                migrations_folder --> normalize_shows[0028_normalize_shows_add_columns.sql]
                migrations_folder --> create_source_tables[0029_create_source_tables.sql]
                migrations_folder --> create_normalized_child_tables[0030_create_normalized_child_tables.sql]
                migrations_folder --> update_show_images[0031_update_show_images_typed.sql]
                migrations_folder --> backfill_normalized_data[0032_backfill_normalized_data.sql]
                migrations_folder --> cleanup_legacy_jsonb[0033_cleanup_legacy_jsonb_columns.sql]
                migrations_folder --> show_images_constraints[0034_show_images_constraints_and_show_flags.sql]
                migrations_folder --> show_images_upsert_rpc[0035_show_images_upsert_rpc.sql]
                migrations_folder --> show_merge_helpers[0036_show_merge_helpers.sql]
                migrations_folder --> update_merge_shows[0038_update_merge_shows_arrays.sql]
                migrations_folder --> drop_child_tables[0039_drop_child_tables.sql]
                migrations_folder --> create_cast_photos[0040_create_cast_photos.sql]
                migrations_folder --> create_cast_fandom[0041_create_cast_fandom_and_extend_cast_photos.sql]
                migrations_folder --> revoke_cast_public[0042_revoke_cast_public_access.sql]
                migrations_folder --> cast_photos_hosted_fields[0043_cast_photos_add_hosted_fields.sql]
                migrations_folder --> create_cast_tmdb[0044_create_cast_tmdb.sql]
                migrations_folder --> add_show_source_metadata[0047_add_show_source_metadata.sql]
                migrations_folder --> create_tmdb_entities[0048_create_tmdb_entities_and_watch_providers.sql]
                migrations_folder --> rename_tmdb_tables[0049_rename_tmdb_dimension_tables.sql]
                migrations_folder --> drop_tmdb_series[0050_drop_or_view_tmdb_imdb_series.sql]
                migrations_folder --> season_images_add_hosted_fields[0051_season_images_add_hosted_fields.sql]
            end
            supabase_folder --> migrations_folder
            
            %% schema_docs Folder in supabase
            subgraph schema_docs_folder["schema_docs"]
                direction TB
                schema_docs_folder --> index[INDEX.md]
                schema_docs_folder --> core_cast_memberships[core.cast_memberships.json]
                schema_docs_folder --> core_memberships_md[core.cast_memberships.md]
                schema_docs_folder --> core_episode_appearances[core.episode_appearances.json]
                schema_docs_folder --> core_episode_appearances_md[core.episode_appearances.md]
                schema_docs_folder --> core_episode_cast[core.episode_cast.json]
                schema_docs_folder --> core_episode_cast_md[core.episode_cast.md]
                schema_docs_folder --> core_episodes[core.episodes.json]
                schema_docs_folder --> core_episodes_md[core.episodes.md]
                schema_docs_folder --> core_people[core.people.json]
                schema_docs_folder --> core_people_md[core.people.md]
                schema_docs_folder --> core_season_images[core.season_images.json]
                schema_docs_folder --> core_season_images_md[core.season_images.md]
                schema_docs_folder --> core_seasons[core.seasons.json]
                schema_docs_folder --> core_seasons_md[core.seasons.md]
                schema_docs_folder --> core_show_cast[core.show_cast.json]
                schema_docs_folder --> core_show_cast_md[core.show_cast.md]
                schema_docs_folder --> core_show_images[core.show_images.json]
                schema_docs_folder --> core_show_images_md[core.show_images.md]
                schema_docs_folder --> core_shows[core.shows.json]
                schema_docs_folder --> core_shows_md[core.shows.md]
                schema_docs_folder --> core_sync_state[core.sync_state.json]
                schema_docs_folder --> core_sync_state_md[core.sync_state.md]
            end
            supabase_folder --> schema_docs_folder

            supabase_folder --> seed[seed.sql]
        end

        %% tests Folder
        subgraph tests_folder["tests"]
            direction TB
            tests_folder --> init[__init__.py]
            tests_folder --> ingestion_folder["ingestion"]
            ingestion_folder --> test_episode_appearances_upsert[test_episode_appearances_upsert.py]
            ingestion_folder --> test_fandom_person_scraper[test_fandom_person_scraper.py]
            ingestion_folder --> test_show_importer_metadata_enrichment[test_show_importer_metadata_enrichment.py]
            ingestion_folder --> test_show_importer_tmdb_details_links[test_show_importer_tmdb_details_links_imdb_show.py]
            ingestion_folder --> test_show_metadata_enricher[test_show_metadata_enricher.py]
            ingestion_folder --> test_tmdb_show_backfill[test_tmdb_show_backfill.py]
            
            tests_folder --> integrations_folder["integrations"]
            integrations_folder --> fandom_folder["fandom"]
            fandom_folder --> test_fandom_infobox_parser[test_fandom_infobox_parser.py]
            integrations_folder --> imdb_folder["imdb"]
            imdb_folder --> test_episodic_client_normalization[test_episodic_client_normalization.py]
            imdb_folder --> test_fullcredits_cast_parser[test_fullcredits_cast_parser.py]
            imdb_folder --> test_imdb_episodes_persistence[test_imdb_episodes_persistence.py]
            imdb_folder --> test_imdb_images[test_imdb_images.py]
            imdb_folder --> test_imdb_list_graphql_client_parsing[test_imdb_list_graphql_client_parsing.py]
            imdb_folder --> test_mediaindex_images[test_mediaindex_images.py]
            imdb_folder --> test_person_gallery_parser[test_person_gallery_parser.py]
            imdb_folder --> test_title_page_metadata[test_title_page_metadata.py]
            integrations_folder --> tmdb_folder["tmdb"]
            tmdb_folder --> test_tmdb_season_enrichment[test_tmdb_season_enrichment.py]
            tmdb_folder --> test_tmdb_tv_details_persistence[test_tmdb_tv_details_persistence.py]
            tmdb_folder --> test_tmdb_tv_images_persistence[test_tmdb_tv_images_persistence.py]
            
            tests_folder --> media_folder["media"]
            media_folder --> test_s3_mirror[test_s3_mirror.py]
            tests_folder --> migrations_folder["migrations"]
            migrations_folder --> test_show_source_metadata_migrations[test_show_source_metadata_migrations.py]
            tests_folder --> repositories_folder["repositories"]
            repositories_folder --> test_cast_photos_upsert[test_cast_photos_upsert.py]
            repositories_folder --> test_pgrst204_retry[test_pgrst204_retry.py]
            repositories_folder --> test_shows_preflight[test_shows_preflight.py]
            tests_folder --> scripts_tests_folder["scripts"]
            scripts_tests_folder --> test_import_shows_from_lists_merge[test_import_shows_from_lists_merge.py]
            scripts_tests_folder --> test_import_shows_from_lists_parsing[test_import_shows_from_lists_parsing.py]
            scripts_tests_folder --> test_import_shows_from_lists_upsert[test_import_shows_from_lists_upsert.py]
            scripts_tests_folder --> test_sync_incremental[test_sync_incremental.py]
            scripts_tests_folder --> test_sync_tmdb_watch_providers[test_sync_tmdb_watch_providers.py]
            tests_folder --> test_api_smoke[test_api_smoke.py]
            tests_folder --> test_discussions_smoke[test_discussions_smoke.py]
            tests_folder --> test_dms_smoke[test_dms_smoke.py]
            tests_folder --> test_ws_realtime_smoke[test_ws_realtime_smoke.py]
            tests_folder --> utils_folder["utils"]
            utils_folder --> test_episode_appearances_aggregation[test_episode_appearances_aggregation.py]
        end
        
        %% trr_backend Folder
        subgraph trr_backend_src["trr_backend"]
            direction TB
            trr_backend_src --> init[__init__.py]
            subgraph db_trr_backend["db"]
                direction TB
                db_trr_backend --> init_db[__init__.py]
                db_trr_backend --> connection[connection.py]
                db_trr_backend --> postgrest_cache[postgrest_cache.py]
                db_trr_backend --> preflight[preflight.py]
                db_trr_backend --> show_images[show_images.py]
                db_trr_backend --> supabase[supabase.py]
            end
            trr_backend_src --> db_trr_backend
            
            subgraph ingestion_trr_backend["ingestion"]
                direction TB
                ingestion_trr_backend --> init_ingestion[__init__.py]
                ingestion_trr_backend --> cast_photo_sources[cast_photo_sources.py]
                ingestion_trr_backend --> fandom_person_scraper[fandom_person_scraper.py]
                ingestion_trr_backend --> imdb_images[imdb_images.py]
                ingestion_trr_backend --> show_importer[show_importer.py]
                ingestion_trr_backend --> show_metadata_enricher[show_metadata_enricher.py]
                ingestion_trr_backend --> showinfo_overrides[showinfo_overrides.py]
                ingestion_trr_backend --> shows_from_lists[shows_from_lists.py]
                ingestion_trr_backend --> tmdb_person_images[tmdb_person_images.py]
                ingestion_trr_backend --> tmdb_show_backfill[tmdb_show_backfill.py]
            end
            trr_backend_src --> ingestion_trr_backend
            
            subgraph integrations_trr_backend["integrations"]
                direction TB
                integrations_trr_backend --> init_integrations[__init__.py]
                integrations_trr_backend --> fandom[fandom.py]
                subgraph imdb_integration["imdb"]
                    direction TB
                    imdb_integration --> init_imdb[__init__.py]
                    imdb_integration --> credits_client[credits_client.py]
                    imdb_integration --> episodic_client[episodic_client.py]
                    imdb_integration --> fullcredits_cast_parser[fullcredits_cast_parser.py]
                    imdb_integration --> list_graphql_client[list_graphql_client.py]
                    imdb_integration --> mediaindex_images[mediaindex_images.py]
                    imdb_integration --> person_gallery[person_gallery.py]
                    imdb_integration --> title_metadata_client[title_metadata_client.py]
                    imdb_integration --> title_page_metadata[title_page_metadata.py]
                end
                integrations_trr_backend --> imdb_integration
                integrations_trr_backend --> tmdb[tmdb]
                integrations_trr_backend --> tmdb_person[tmdb_person.py]
            end
            trr_backend_src --> integrations_trr_backend
            
            subgraph media_trr_backend["media"]
                direction TB
                media_trr_backend --> init_media[__init__.py]
                media_trr_backend --> s3_mirror[s3_mirror.py]
            end
            trr_backend_src --> media_trr_backend
            
            subgraph models_trr_backend["models"]
                direction TB
                models_trr_backend --> init_models[__init__.py]
                models_trr_backend --> cast_photos[cast_photos.py]
                models_trr_backend --> shows[shows.py]
            end
            trr_backend_src --> models_trr_backend
            
            subgraph repositories_trr_backend["repositories"]
                direction TB
                repositories_trr_backend --> init_repositories[__init__.py]
                repositories_trr_backend --> cast_fandom[cast_fandom.py]
                repositories_trr_backend --> cast_photos_repo[cast_photos.py]
                repositories_trr_backend --> cast_tmdb[cast_tmdb.py]
                repositories_trr_backend --> episode_appearances[episode_appearances.py]
                repositories_trr_backend --> episodes[episodes.py]
                repositories_trr_backend --> imdb_series[imdb_series.py]
                repositories_trr_backend --> people[people.py]
                repositories_trr_backend --> season_images[season_images.py]
                repositories_trr_backend --> seasons[seasons.py]
                repositories_trr_backend --> show_cast[show_cast.py]
                repositories_trr_backend --> show_images[show_images.py]
                repositories_trr_backend --> shows[shows.py]
                repositories_trr_backend --> sync_state[sync_state.py]
                repositories_trr_backend --> tmdb_series[tmdb_series.py]
            end
            trr_backend_src --> repositories_trr_backend
            
            subgraph utils_trr_backend["utils"]
                direction TB
                utils_trr_backend --> init_utils[__init__.py]
                utils_trr_backend --> env[env.py]
                utils_trr_backend --> episode_appearances_utils[episode_appearances.py]
            end
            trr_backend_src --> utils_trr_backend
        end
    end
```