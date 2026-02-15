# Mermaid Diagram

```mermaid
graph TD
    %% Repository Structure
    subgraph trr-backend
        root[trr-backend]
        
        subgraph claude[CLAUDE]
            claude_cmds[Commands]
            claude_hooks[Hooks]
            claude_plans[Plans]
            root --> claude_node
            claude_node --> claude_cmds
            claude_cmds --> trr_impl[trr-impl.md]
            claude_cmds --> trr_plan[trr-plan.md]
            claude_cmds --> trr_pr[trr-pr.md]
            claude_cmds --> trr_spec[trr-spec.md]
            claude_cmds --> trr_validate[trr-validate.md]
            claude_cmds --> trr_wt_new[trr-wt-new.md]
            claude_node --> claude_hooks
            claude_hooks --> before_bash[before-bash.md]
            claude_hooks --> on_stop[on-stop.md]
            claude_node --> claude_plans
            claude_plans --> v2_runs[v2-runs-implementation.md]
        end
        
        subgraph config[CONFIG]
            root --> config_node
            config_node --> wt[wt.toml]
        end
        
        root --> dockerignore[.dockerignore]
        root --> env_example[.env.example]
        
        subgraph github[.GITHUB]
            root --> github_node
            github_node --> workflows[Workflows]
            workflows --> ci[ci.yml]
            workflows --> mirror[mirror-media-assets.yml]
            workflows --> repo_map[repo_map.yml]
            workflows --> secret_scan[secret-scan.yml]
        end
        
        root --> gitignore[.gitignore]
        root --> gitleaks[.gitleaks.toml]
        root --> python_version[.python-version]
        root --> agents[AGENTS.md]
        root --> branching_strategy[BRANCHING_STRATEGY.md]
        root --> claude_doc[CLAUDE.md]
        root --> contributing[CONTRIBUTING.md]
        root --> dockerfile[Dockerfile]
        root --> makefile[Makefile]
        root --> readme[README.md]
        root --> repo_structure[REPO_STRUCTURE.md]
        
        subgraph api[API]
            root --> api_node
            api_node --> init[__init__.py]
            api_node --> auth[auth.py]
            api_node --> deps[deps.py]
            api_node --> main[main.py]
            
            subgraph realtime[Realtime]
                api_node --> realtime_node
                realtime_node --> init_realtime[__init__.py]
                realtime_node --> broker[broker.py]
                realtime_node --> events[events.py]
            end
            
            subgraph routers[ROUTERS]
                api_node --> routers_node
                routers_node --> init_routers[__init__.py]
                routers_node --> admin_asset_flags[admin_asset_flags.py]
                routers_node --> admin_cast[admin_cast.py]
                routers_node --> admin_cast_photos[admin_cast_photos.py]
                routers_node --> admin_image_counts[admin_image_counts.py]
                routers_node --> admin_media_assets[admin_media_assets.py]
                routers_node --> admin_person_images[admin_person_images.py]
                routers_node --> admin_scrape[admin_scrape.py]
                routers_node --> admin_show_bravo[admin_show_bravo.py]
                routers_node --> admin_show_links[admin_show_links.py]
                routers_node --> admin_show_roles[admin_show_roles.py]
                routers_node --> admin_show_sync[admin_show_sync.py]
                routers_node --> discussions[discussions.py]
                routers_node --> dms[dms.py]
                routers_node --> screenalytics[screenalytics.py]
                routers_node --> screenalytics_runs[screenalytics_runs_v2.py]
                routers_node --> shows[shows.py]
                routers_node --> socials[socials.py]
                routers_node --> surveys[surveys.py]
                routers_node --> ws[ws.py]
            end
            
            api_node --> screenalytics_auth[screenalytics_auth.py]
        end
        
        root --> backfill_tmdb[backfill_tmdb_show_details.py]
        
        subgraph docs[DOCS]
            root --> docs_node
            docs_node --> history_purge[HISTORY_PURGE.md]
            docs_node --> readme_local[README_local.md]
            
            subgraph repository[Repository]
                docs_node --> repository_node
                repository_node --> repo_readme[README.md]
                repository_node --> diagrams[Diagrams]
                diagrams --> git_workflow[git_workflow.md]
                diagrams --> system_maps[system_maps.md]
                repository_node --> generated[Generated]
                generated --> gitkeep[.gitkeep]
                generated --> code_import_graph[CODE_IMPORT_GRAPH.md]
                generated --> repo_structure_generated[REPO_STRUCTURE.md]
                generated --> repo_structure_mermaid[REPO_STRUCTURE.mermaid.md]
                generated --> scripts_flow[SCRIPTS_FLOW.md]
                generated --> rendered[Rendered]
                rendered --> code_import_graph_svg[CODE_IMPORT_GRAPH-1.svg]
                rendered --> repo_structure_mermaid_svg[REPO_STRUCTURE.mermaid-1.svg]
                rendered --> scripts_flow_svg[SCRIPTS_FLOW-1.svg]
                rendered --> git_workflow_svg[git_workflow-1.svg]
                rendered --> system_maps_svg[system_maps-1.svg]
                rendered --> system_maps_svg_2[system_maps-2.svg]
            end
            
            docs_node --> security[SECURITY.md]
            docs_node --> ai[AI]
            ai --> handoff[HANDOFF.md]
            docs_node --> api_docs[API]
            api_docs --> run[run.md]
            docs_node --> architecture[ARCHITECTURE]
            architecture --> imdb_fullcredits_resilience_plan[imdb_fullcredits_resilience_implementation_plan.md]
            architecture --> imdb_fullcredits_resilience_spec[imdb_fullcredits_resilience_spec.md]
            architecture --> imdb_graphql_migration_spec[imdb_graphql_migration_spec.md]
            architecture --> integrations[integrations.md]
            architecture --> pipeline[pipeline.md]
            docs_node --> architecture_md[architecture.md]
            
            subgraph cloud[CLOUD]
                docs_node --> cloud_node
                cloud_node --> cloud_setup[cloud_setup.md]
                cloud_node --> quick_cloud_setup[quick_cloud_setup.md]
                cloud_node --> setup_codespaces_credentials[setup_codespaces_credentials.md]
            end
            
            subgraph cross_collab[CROSS-COLLAB]
                docs_node --> cross_collab_node
                cross_collab_node --> readme_collab[README.md]
                cross_collab_node --> task1[TASK1]
                task1 --> other_projects1[OTHER_PROJECTS.md]
                task1 --> plan1[PLAN.md]
                cross_collab_node --> task2[TASK2]
                task2 --> other_projects2[OTHER_PROJECTS.md]
                task2 --> plan2[PLAN.md]
                cross_collab_node --> task3[TASK3]
                task3 --> other_projects3[OTHER_PROJECTS.md]
                task3 --> plan3[PLAN.md]
                cross_collab_node --> task4[TASK4]
                task4 --> other_projects4[OTHER_PROJECTS.md]
                task4 --> plan4[PLAN.md]
                task4 --> status4[STATUS.md]
                cross_collab_node --> task5[TASK5]
                task5 --> other_projects5[OTHER_PROJECTS.md]
                task5 --> plan5[PLAN.md]
                task5 --> status5[STATUS.md]
                cross_collab_node --> task6[TASK6]
                task6 --> other_projects6[OTHER_PROJECTS.md]
                task6 --> plan6[PLAN.md]
                task6 --> status6[STATUS.md]
                cross_collab_node --> task7[TASK7]
                task7 --> other_projects7[OTHER_PROJECTS.md]
                task7 --> plan7[PLAN.md]
                task7 --> status7[STATUS.md]
            end
            
            subgraph db[DB]
                docs_node --> db_node
                db_node --> commands[commands.md]
                db_node --> schema[schema.md]
                db_node --> verification[verification.md]
            end
            
            subgraph deploy[DEPLOY]
                docs_node --> deploy_node
                deploy_node --> cloud_run[cloud_run.md]
            end
            
            subgraph images[IMAGES]
                docs_node --> images_node
                images_node --> debug_imdb_credits[debug_imdb_credits.png]
            end
            
            subgraph legacy[LEGACY]
                docs_node --> legacy_node
                legacy_node --> legacy_readme[README.md]
                legacy_node --> legacy_local[README_local_google_sheets.md]
                legacy_node --> sheet_edit_mapping[SHEET_EDIT_MAPPING.md]
                legacy_node --> legacy_architecture[architecture_google_sheets.md]
                legacy_node --> cloud_quick_setup_google_sheets[cloud_quick_setup_google_sheets.md]
                legacy_node --> cloud_setup_google_sheets[cloud_setup_google_sheets.md]
                legacy_node --> codespaces_google_credentials[codespaces_google_credentials.md]
                legacy_node --> google_sheets_pipeline[google_sheets_pipeline.md]
            end
            
            subgraph plans[PLANS]
                docs_node --> plans_node
                plans_node --> plan1[2026-01-28-surveys-supabase-auth.md]
                plans_node --> plan2[repo_cleanup.md]
            end
            
            subgraph runbooks[RUNBOOKS]
                docs_node --> runbooks_node
                runbooks_node --> credits_v2_rollout[credits_v2_rollout.md]
                runbooks_node --> postgrest_schema_cache[postgrest_schema_cache.md]
                runbooks_node --> rhoslc_show_admin_backfill[rhoslc-show-admin-backfill.md]
                runbooks_node --> show_import_job[show_import_job.md]
            end
            
            subgraph workflows_docs[WORKFLOWS]
                docs_node --> workflows_docs_node
                workflows_docs_node --> vibe_coding[VIBE_CODING.md]
            end
        end
        
        root --> pytest[pytest.ini]
        root --> requirements[requirements.txt]
        root --> resolve_tmdb_ids[resolve_tmdb_ids_via_find.py]
        root --> ruff[ruff.toml]
        
        subgraph scripts[SCRIPTS]
            root --> scripts_node
            scripts_node --> readme_scripts[README.md]
            scripts_node --> sync_common[_sync_common.py]
            subgraph backfill_scripts[BACKFILL]
                scripts_node --> backfill_scripts_node
                backfill_scripts_node --> backfill_credits[backfill_credits.py]
                backfill_scripts_node --> backfill_media_assets[backfill_media_assets.py]
                backfill_scripts_node --> backfill_tmdb[backfill_tmdb_show_details.py]
            end
            
            subgraph cleanup[CLEANUP]
                scripts_node --> cleanup_node
                cleanup_node --> cleanup_fandom_mismatches[cleanup_fandom_mismatches.py]
            end
            
            scripts_node --> cleanup_expired_media_uploads[cleanup_expired_media_uploads.py]
            
            subgraph db_scripts[DB]
                scripts_node --> db_scripts_node
                db_scripts_node --> readme_db[README.md]
                db_scripts_node --> guard_core_schema[guard_core_schema.sql]
                db_scripts_node --> reload_postgrest_schema[reload_postgrest_schema.sql]
                db_scripts_node --> run_sql[run_sql.sh]
                db_scripts_node --> verify_media_unification[verify_media_unification.sql]
                db_scripts_node --> verify_pre_0033_cleanup[verify_pre_0033_cleanup.sql]
            end
            
            subgraph dev[DEV]
                scripts_node --> dev_node
                dev_node --> doctor[doctor.py]
            end
            
            subgraph enrich[ENRICH]
                scripts_node --> enrich_node
                enrich_node --> enrich_show_cast[enrich_show_cast.py]
                enrich_node --> imdb_show_enrichment[imdb_show_enrichment.py]
                enrich_node --> rhoslc_fandom_enrichment[rhoslc_fandom_enrichment.py]
            end
            
            scripts_node --> fix_repo_structure_mermaid[fix_repo_structure_mermaid.py]
            scripts_node --> generate_repo_mermaid[generate_repo_mermaid.py]
            
            subgraph import_scripts[IMPORT]
                scripts_node --> import_scripts_node
                import_scripts_node --> import_fandom_gallery_photos[ import_fandom_gallery_photos.py]
                import_scripts_node --> import_imdb_cast_episode_appearances[import_imdb_cast_episode_appearances.py]
                import_scripts_node --> import_shows_from_lists[import_shows_from_lists.py]
                import_scripts_node --> run_show_import_job[run_show_import_job.py]
            end
            
            scripts_node --> legacy[legacy]
            legacy_node --> legacy_test_connection[test_connection.py]
            
            subgraph media[MEDIA]
                scripts_node --> media_node
                media_node --> readme_media[README.md]
                media_node --> backfill_media_asset_variants[backfill_media_asset_variants.py]
                media_node --> cleanup_expired_media_uploads[cleanup_expired_media_uploads.py]
                media_node --> mirror_cast_photos_to_s3[mirror_cast_photos_to_s3.py]
                media_node --> mirror_media_assets_to_s3[mirror_media_assets_to_s3.py]
                media_node --> mirror_show_images_to_s3[mirror_show_images_to_s3.py]
                media_node --> rebuild_hosted_urls[rebuild_hosted_urls.py]
                media_node --> repair_cast_photo_hosts[repair_cast_photo_hosts.py]
            end
            
            scripts_node --> mirror_cast_photos_to_s3[mirror_cast_photos_to_s3.py]
            scripts_node --> mirror_media_assets_to_s3[mirror_media_assets_to_s3.py]
            scripts_node --> mirror_show_images_to_s3[mirror_show_images_to_s3.py]
            scripts_node --> rebuild_hosted_urls[rebuild_hosted_urls.py]
            scripts_node --> reload_postgrest_schema[reload_postgrest_schema.sh]
            scripts_node --> reload_postgrest_schema_sql[reload_postgrest_schema.sql]
            scripts_node --> resolve_tmdb_ids_via_find[resolve_tmdb_ids_via_find.py]

            scripts_node --> rhoslc_fandom_enrichment[rhoslc_fandom_enrichment.py]
            scripts_node --> run_show_import_job[run_show_import_job.py]
            
            subgraph socials[SOCIALS]
                scripts_node --> socials_node
                socials_node --> init_socials[__init__.py]
                socials_node --> instagram[instagram]
                instagram --> init_instagram[__init__.py]
                instagram --> instagram_cookies[instagram_cookies.example.json]
                instagram --> scrape[scrape.py]
                
                socials_node --> tiktok[tiktok]
                tiktok --> init_tiktok[__init__.py]
                tiktok --> scrape_tiktok[scrape.py]
                
                socials_node --> twitter[twitter]
                twitter --> init_twitter[__init__.py]
                twitter --> scrape_twitter[scrape.py]
                
                socials_node --> youtube[youtube]
                youtube --> init_youtube[__init__.py]
                youtube --> scrape_youtube[scrape.py]
            end
            
            scripts_node --> supabase[supabase]
            supabase_node --> generate_schema_docs[generate_schema_docs.py]
            
            subgraph sync[SYNC]
                scripts_node --> sync_node
                sync_node --> sync_all_tables[sync_all_tables.py]
                sync_node --> sync_cast_batch[sync_cast_batch.py]
                sync_node --> sync_cast_photos[sync_cast_photos.py]
                sync_node --> sync_episode_appearances[sync_episode_appearances.py]
                sync_node --> sync_episodes[sync_episodes.py]
                sync_node --> sync_people[sync_people.py]
                sync_node --> sync_season_episode_images[sync_season_episode_images.py]
                sync_node --> sync_seasons[sync_seasons.py]
                sync_node --> sync_seasons_episodes[sync_seasons_episodes.py]
                sync_node --> sync_show_batch[sync_show_batch.py]
                sync_node --> sync_show_cast[sync_show_cast.py]
                sync_node --> sync_show_complete[sync_show_complete.py]
                sync_node --> sync_show_images[sync_show_images.py]
                sync_node --> sync_shows[sync_shows.py]
                sync_node --> sync_shows_all[sync_shows_all.py]
                sync_node --> sync_tmdb_person_images[sync_tmdb_person_images.py]
                sync_node --> sync_tmdb_show_entities[sync_tmdb_show_entities.py]
                sync_node --> sync_tmdb_watch_providers[sync_tmdb_watch_providers.py]
            end
            
            subgraph verify[VERIFY]
                scripts_node --> verify_node
                verify_node --> validate_supabase_timeouts[validate_supabase_timeouts.py]
                verify_node --> verify_credits_parity[verify_credits_parity.py]
                verify_node --> verify_media_unification[verify_media_unification.py]
                verify_node --> verify_schema[verify_schema.py]
            end
        end
        
        root --> skills[SKILLS]
        skills --> database_designer[database-designer]
        database_designer --> skill[SKILL.md]
        database_designer --> references[References]
        references --> examples[examples.md]
        references --> playbooks[playbooks.md]
        references --> repo_context[repo-context.md]
        references --> templates[templates.sql]
        references --> tooling[tooling.md]
        
        root --> start_api[start-api.sh]
        
        subgraph supabase[SUPABASE]
            root --> supabase_node
            supabase_node --> supabase_gitignore[.gitignore]
            supabase_node --> config_supabase[config.toml]
            
            subgraph migrations[MIGRATIONS]
                supabase_node --> migrations_node
                migrations_node --> migration1[0001_init.sql]
                migrations_node --> migration2[0002_social.sql]
                migrations_node --> migration3[0003_dms.sql]
                migrations_node --> migration4[0004_core_shows.sql]
                migrations_node --> migration5[0005_show_images.sql]
                migrations_node --> migration6[0006_show_images_grants.sql]
                migrations_node --> migration7[0007_core_shows_tmdb_id.sql]
                migrations_node --> migration8[0008_show_images_tmdb_id.sql]
                migrations_node --> migration9[0009_show_images_view.sql]
                migrations_node --> migration10[0010_show_images_no_votes.sql]
                migrations_node --> migration11[0011_show_images_view_no_votes.sql]
                migrations_node --> migration12[0012_seasons_and_episodes.sql]
                migrations_node --> migration13[0013_season_images.sql]
                migrations_node --> migration14[0014_show_seasons_view.sql]
                migrations_node --> migration15[0015_seasons_show_name.sql]
                migrations_node --> migration16[0016_seasons_episode_id_arrays.sql]
                migrations_node --> migration17[0017_episodes_show_name.sql]
                migrations_node --> migration18[0018_imdb_cast_episode_appearances.sql]
                migrations_node --> migration19[0019_imdb_cast_grants.sql]
                migrations_node --> migration20[0020_reorder_show_tables.sql]
                migrations_node --> migration21[0021_reorder_people_cast_seasons_episodes.sql]
                migrations_node --> migration22[0022_episode_appearances_export_view.sql]
                migrations_node --> migration23[0023_episode_appearances_export_view_total_episodes.sql]
                migrations_node --> migration24[0024_episode_appearances_aggregate.sql]
                migrations_node --> migration25[0025_sync_state.sql]
                migrations_node --> migration26[0026_add_imdb_meta_to_core_shows.sql]
                migrations_node --> migration27[0027_show_images_media_sources.sql]
                migrations_node --> migration28[0028_normalize_shows_add_columns.sql]
                migrations_node --> migration29[0029_create_source_tables.sql]
                migrations_node --> migration30[0030_create_normalized_child_tables.sql]
                migrations_node --> migration31[0031_update_show_images_typed.sql]
                migrations_node --> migration32[0032_backfill_normalized_data.sql]
                migrations_node --> migration33[0033_cleanup_legacy_jsonb_columns.sql]
                migrations_node --> migration34[0034_show_images_constraints_and_show_flags.sql]
                migrations_node --> migration35[0035_show_images_upsert_rpc.sql]
                migrations_node --> migration36[0036_show_merge_helpers.sql]
                migrations_node --> migration37[0037_collapse_show_attributes.sql]
                migrations_node --> migration38[0038_update_merge_shows_arrays.sql]
                migrations_node --> migration39[0039_drop_child_tables.sql]
                migrations_node --> migration40[0040_create_cast_photos.sql]
                migrations_node --> migration41[0041_create_cast_fandom_and_extend_cast_photos.sql]
                migrations_node --> migration42[0042_revoke_cast_public_access.sql]
                migrations_node --> migration43[0043_cast_photos_add_hosted_fields.sql]
                migrations_node --> migration44[0044_create_cast_tmdb.sql]
                migrations_node --> migration45[0045_show_images_add_hosted_fields.sql]
                migrations_node --> migration46[0046_cast_photos_allow_tmdb_source.sql]
                migrations_node --> migration47[0047_add_show_source_metadata.sql]
                migrations_node --> migration48[0048_create_tmdb_entities_and_watch_providers.sql]
                migrations_node --> migration49[0049_rename_tmdb_dimension_tables.sql]
                migrations_node --> migration50[0050_drop_or_view_tmdb_imdb_series.sql]
                migrations_node --> migration51[0051_season_images_add_hosted_fields.sql]
                migrations_node --> migration52[0052_season_images_add_metadata_fields.sql]
                migrations_node --> migration53[0053_add_show_cast_source_tracking.sql]
                migrations_node --> migration54[0054_show_images_upsert_rpc_remove_votes.sql]
                migrations_node --> migration55[0055_expand_show_cast_source_types_graphql.sql]
                migrations_node --> migration56[0056_create_person_images.sql]
                migrations_node --> migration57[0057_add_alternative_names_to_shows.sql]
                migrations_node --> migration58[0058_create_media_assets.sql]
                migrations_node --> migration59[0059_create_media_links.sql]
                migrations_node --> migration60[0060_create_media_served_views.sql]
                migrations_node --> migration61[0061_add_media_assets_ingest_fields.sql]
                migrations_node --> migration62[0062_create_v_media_ingest_summary.sql]
                migrations_node --> migration63[0063_set_primary_media_link_rpc.sql]
                migrations_node --> migration64[0064_create_media_uploads.sql]
                migrations_node --> migration65[0065_create_credits_tables.sql]
                migrations_node --> migration66[0066_create_credits_validation_views.sql]
                migrations_node --> migration67[0067_create_episode_images.sql]
                migrations_node --> migration68[0068_prep_helpers.sql]
                migrations_node --> migration69[0069_sources.sql]
                migrations_node --> migration70[0070_external_ids.sql]
                migrations_node --> migration71[0071_source_snapshots.sql]
                migrations_node --> migration72[0072_media_constraints.sql]
                migrations_node --> migration73[0073_backfill_external_ids.sql]
                migrations_node --> migration74[0074_backfill_source_snapshots.sql]
                migrations_node --> migration75[0075_backfill_media_links.sql]
                migrations_node --> migration76[0076_primary_assignment.sql]
                migrations_node --> migration77[0077_validation_gates.sql]
                migrations_node --> migration78[0078_compat_views.sql]
                migrations_node --> migration79[0079_deprecations.sql]
                migrations_node --> migration80[0080_bridge_legacy_media_links.sql]
                migrations_node --> migration81[0081_bridge_show_source_snapshots.sql]
                migrations_node --> migration82[0082_create_show_alternative_names.sql]
                migrations_node --> migration83[0083_grant_show_source_history_sequences.sql]
                migrations_node --> migration84[0084_grant_media_assets_links.sql]
                migrations_node --> migration85[0085_grant_media_uploads.sql]
                migrations_node --> migration86[0086_create_pipeline_schema.sql]
                migrations_node --> migration87[0087_screenalytics_cast_views.sql]
                migrations_node --> migration88[0088_person_images_view.sql]
                migrations_node --> migration89[0089_survey_response_unique_per_user.sql]
                migrations_node --> migration90[0090_survey_submit_response_rpc.sql]
                migrations_node --> migration92[0092_survey_slug_column.sql]
                migrations_node --> migration93[0093_create_screenalytics_v2_runs.sql]
                migrations_node --> migration94[0094_fix_bridge_cast_photos_updates.sql]
                migrations_node --> migration95[0095_cast_overrides.sql]
                migrations_node --> migration96[0096_image_archive_columns.sql]
                migrations_node --> migration97[0097_image_audit_log.sql]
                migrations_node --> migration98[0098_fix_bridge_hosted_sha256_conflict.sql]
                migrations_node --> migration99[0099_admin_cast_photo_people_tags.sql]
                migrations_node --> migration100[0100_facebank_seed_media_links.sql]
                migrations_node --> migration101[0101_social_scrape_tables.sql]
                migrations_node --> migration102[0102_screenalytics_face_bank_images.sql]
                migrations_node --> migration103[0103_screenalytics_video_asset_cast_candidates.sql]
                migrations_node --> migration104[0104_screenalytics_v1_operational_tables.sql]
                migrations_node --> migration105[0105_screenalytics_outbox_events.sql]
                migrations_node --> migration106[0106_drop_games_schema.sql]
                migrations_node --> migration107[0107_drop_legacy_cast_tables.sql]
                migrations_node --> migration108[0108_modify_core_shows_consolidate_columns.sql]
                migrations_node --> migration109[0109_enrich_core_people_multisource.sql]
                migrations_node --> migration110[0110_enrich_core_credit_occurrences.sql]
                migrations_node --> migration111[0111_add_social_columns_dimension_tables.sql]
                migrations_node --> migration112[0112_expand_people_overrides_handles.sql]
                migrations_node --> migration113[0113_extend_social_scrape_jobs_platforms.sql]
                migrations_node --> migration114[0114_create_core_v_cast_summary.sql]
                migrations_node --> migration115[0115_reconcile_screenalytics_v2_tables.sql]
                migrations_node --> migration116[0116_archive_media_assets_and_show_images.sql]
                migrations_node --> migration117[0117_add_bravo_source.sql]
                migrations_node --> migration118[0118_social_season_analytics.sql]
                migrations_node --> migration119[0119_create_media_asset_variants.sql]
                migrations_node --> migration120[0120_show_admin_links_and_roles.sql]
                migrations_node --> migration121[0121_social_scrape_runs.sql]
                migrations_node --> migration122[0122_social_scrape_jobs_queue_fields.sql]
                migrations_node --> migration123[0123_social_scrape_jobs_queue_indexes.sql]
            end
            
            supabase_node --> schema_docs[SCHEMA_DOCS]
            schema_docs --> index[INDEX.md]
            schema_docs --> core_cast_fandom[core.cast_fandom.json]
            schema_docs --> core_cast_fandom_md[core.cast_fandom.md]
            schema_docs --> core_cast_photos[core.cast_photos.json]
            schema_docs --> core_cast_photos_md[core.cast_photos.md]
            schema_docs --> core_cast_tmdb[core.cast_tmdb.json]
            schema_docs --> core_cast_tmdb_md[core.cast_tmdb.md]
            schema_docs --> core_credit_occurrences[core.credit_occurrences.json]
            schema_docs --> core_credit_occurrences_md[core.credit_occurrences.md]
            schema_docs --> core_credits[core.credits.json]
            schema_docs --> core_credits_md[core.credits.md]
            schema_docs --> core_entity_links[core.entity_links.json]
            schema_docs --> core_entity_links_md[core.entity_links.md]
            schema_docs --> core_episode_external_ids[core.episode_external_ids.json]
            schema_docs --> core_episode_external_ids_md[core.episode_external_ids.md]
            schema_docs --> core_episode_images[core.episode_images.json]
            schema_docs --> core_episode_images_md[core.episode_images.md]
            schema_docs --> core_episode_source_history[core.episode_source_history.json]
            schema_docs --> core_episode_source_history_md[core.episode_source_history.md]
            schema_docs --> core_episode_source_latest[core.episode_source_latest.json]
            schema_docs --> core_episode_source_latest_md[core.episode_source_latest.md]
            schema_docs --> core_episodes[core.episodes.json]
            schema_docs --> core_episodes_md[core.episodes.md]
            schema_docs --> core_external_id_conflicts[core.external_id_conflicts.json]
            schema_docs --> core_external_id_conflicts_md[core.external_id_conflicts.md]
            schema_docs --> core_media_asset_variants[core.media_asset_variants.json]
            schema_docs --> core_media_asset_variants_md[core.media_asset_variants.md]
            schema_docs --> core_media_assets[core.media_assets.json]
            schema_docs --> core_media_assets_md[core.media_assets.md]
            schema_docs --> core_media_links[core.media_links.json]
            schema_docs --> core_media_links_md[core.media_links.md]
            schema_docs --> core_media_uploads[core.media_uploads.json]
            schema_docs --> core_media_uploads_md[core.media_uploads.md]
            schema_docs --> core_networks[core.networks.json]
            schema_docs --> core_networks_md[core.networks.md]
            schema_docs --> core_people[core.people.json]
            schema_docs --> core_people_md[core.people.md]
            schema_docs --> core_people_overrides[core.people_overrides.json]
            schema_docs --> core_people_overrides_md[core.people_overrides.md]
            schema_docs --> core_person_external_ids[core.person_external_ids.json]
            schema_docs --> core_person_external_ids_md[core.person_external_ids.md]
            schema_docs --> core_person_images[core.person_images.json]
            schema_docs --> core_person_images_md[core.person_images.md]
            schema_docs --> core_person_source_history[core.person_source_history.json]
            schema_docs --> core_person_source_history_md[core.person_source_history.md]
            schema_docs --> core_person_source_latest[core.person_source_latest.json]
            schema_docs --> core_person_source_latest_md[core.person_source_latest.md]
            schema_docs --> core_production_companies[core.production_companies.json]
            schema_docs --> core_production_companies_md[core.production_companies.md]
            schema_docs --> core_season_external_ids[core.season_external_ids.json]
            schema_docs --> core_season_external_ids_md[core.season_external_ids.md]
            schema_docs --> core_season_images[core.season_images.json]
            schema_docs --> core_season_images_md[core.season_images.md]
            schema_docs --> core_season_source_history[core.season_source_history.json]
            schema_docs --> core_season_source_history_md[core.season_source_history.md]
            schema_docs --> core_season_source_latest[core.season_source_latest.json]
            schema_docs --> core_season_source_latest_md[core.season_source_latest.md]
            schema_docs --> core_seasons[core.seasons.json]
            schema_docs --> core_seasons_md[core.seasons.md]
            schema_docs --> core_show_alternative_names[core.show_alternative_names.json]
            schema_docs --> core_show_alternative_names_md[core.show_alternative_names.md]
            schema_docs --> core_show_cast_overrides[core.show_cast_overrides.json]
            schema_docs --> core_show_cast_overrides_md[core.show_cast_overrides.md]
            schema_docs --> core_show_cast_role_assignments[core.show_cast_role_assignments.json]
            schema_docs --> core_show_cast_role_assignments_md[core.show_cast_role_assignments.md]
            schema_docs --> core_show_external_ids[core.show_external_ids.json]
            schema_docs --> core_show_external_ids_md[core.show_external_ids.md]
            schema_docs --> core_show_images[core.show_images.json]
            schema_docs --> core_show_images_md[core.show_images.md]
            schema_docs --> core_show_role_catalog[core.show_role_catalog.json]
            schema_docs --> core_show_role_catalog_md[core.show_role_catalog.md]
            schema_docs --> core_show_source_history[core.show_source_history.json]
            schema_docs --> core_show_source_history_md[core.show_source_history.md]
            schema_docs --> core_show_source_latest[core.show_source_latest.json]
            schema_docs --> core_show_source_latest_md[core.show_source_latest.md]
            schema_docs --> core_show_watch_providers[core.show_watch_providers.json]
            schema_docs --> core_show_watch_providers_md[core.show_watch_providers.md]
            schema_docs --> core_shows[core.shows.json]
            schema_docs --> core_shows_md[core.shows.md]
            schema_docs --> core_sources[core.sources.json]
            schema_docs --> core_sources_md[core.sources.md]
            schema_docs --> core_sync_state[core.sync_state.json]
            schema_docs --> core_sync_state_md[core.sync_state.md]
            schema_docs --> core_watch_providers[core.watch_providers.json]
            schema_docs --> core_watch_providers_md[core.watch_providers.md]
            schema_docs --> diagrams[Diagrams]
            diagrams --> core_cast_fandom_mermaid[core.cast_fandom.mermaid.md]
            diagrams --> core_cast_photos_mermaid[core.cast_photos.mermaid.md]
            diagrams --> core_cast_tmdb_mermaid[core.cast_tmdb.mermaid.md]
            diagrams --> core_credit_occurrences_mermaid[core.credit_occurrences.mermaid.md]
            diagrams --> core_credits_mermaid[core.credits.mermaid.md]
            diagrams --> core_entity_links_mermaid[core.entity_links.mermaid.md]
            diagrams --> core_episode_external_ids_mermaid[core.episode_external_ids.mermaid.md]
            diagrams --> core_episode_images_mermaid[core.episode_images.mermaid.md]
            diagrams --> core_episode_source_history_mermaid[core.episode_source_history.mermaid.md]
            diagrams --> core_episode_source_latest_mermaid[core.episode_source_latest.mermaid.md]
            diagrams --> core_episodes_mermaid[core.episodes.mermaid.md]
            diagrams --> core_external_id_conflicts_mermaid[core.external_id_conflicts.mermaid.md]
            diagrams --> core_media_asset_variants_mermaid[core.media_asset_variants.mermaid.md]
            diagrams --> core_media_assets_mermaid[core.media_assets.mermaid.md]
            diagrams --> core_media_links_mermaid[core.media_links.mermaid.md]
            diagrams --> core_media_uploads_mermaid[core.media_uploads.mermaid.md]
            diagrams --> core_networks_mermaid[core.networks.mermaid.md]
            diagrams --> core_people_mermaid[core.people.mermaid.md]
            diagrams --> core_people_overrides_mermaid[core.people_overrides.mermaid.md]
            diagrams --> core_person_external_ids_mermaid[core.person_external_ids.mermaid.md]
            diagrams --> core_person_images_mermaid[core.person_images.mermaid.md]
            diagrams --> core_person_source_history_mermaid[core.person_source_history.mermaid.md]
            diagrams --> core_person_source_latest_mermaid[core.person_source_latest.mermaid.md]
            diagrams --> core_production_companies_mermaid[core.production_companies.mermaid.md]
            diagrams --> core_season_external_ids_mermaid[core.season_external_ids.mermaid.md]
            diagrams --> core_season_images_mermaid[core.season_images.mermaid.md]
            diagrams --> core_season_source_history_mermaid[core.season_source_history.mermaid.md]
            diagrams --> core_season_source_latest_mermaid[core.season_source_latest.mermaid.md]
            diagrams --> core_seasons_mermaid[core.seasons.mermaid.md]
            diagrams --> core_show_alternative_names_mermaid[core.show_alternative_names.mermaid.md]
            diagrams --> core_show_cast_overrides_mermaid[core.show_cast_overrides.mermaid.md]
            diagrams --> core_show_cast_role_assignments_mermaid[core.show_cast_role_assignments.mermaid.md]
            diagrams --> core_show_external_ids_mermaid[core.show_external_ids.mermaid.md]
            diagrams --> core_show_images_mermaid[core.show_images.mermaid.md]
            diagrams --> core_show_role_catalog_mermaid[core.show_role_catalog.mermaid.md]
            diagrams --> core_show_source_history_mermaid[core.show_source_history.mermaid.md]
            diagrams --> core_show_source_latest_mermaid[core.show_source_latest.mermaid.md]
            diagrams --> core_show_watch_providers_mermaid[core.show_watch_providers.mermaid.md]
            diagrams --> core_shows_mermaid[core.shows.mermaid.md]
            diagrams --> core_sources_mermaid[core.sources.mermaid.md]
            diagrams --> core_sync_state_mermaid[core.sync_state.mermaid.md]
            diagrams --> core_watch_providers_mermaid[core.watch_providers.mermaid.md]
        end
        
        root --> test_connection[test_connection.py]
        
        subgraph tests[TESTS]
            root --> tests_node
            tests_node --> init_tests[__init__.py]
            
            subgraph api_tests[API]
                tests_node --> api_tests_node
                api_tests_node --> routers_tests[ROUTERS]
                routers_tests --> init_routers_tests[__init__.py]
                routers_tests --> test_admin_asset_flags[test_admin_asset_flags.py]
                routers_tests --> test_admin_image_counts_fallback[test_admin_image_counts_fallback.py]
                routers_tests --> test_admin_person_images[test_admin_person_images.py]
                routers_tests --> test_admin_scrape_contracts[test_admin_scrape_contracts.py]
                routers_tests --> test_admin_show_bravo[test_admin_show_bravo.py]
                routers_tests --> test_admin_show_sync[test_admin_show_sync.py]
                routers_tests --> test_socials_season_analytics[test_socials_season_analytics.py]
                api_tests_node --> test_auth[test_auth.py]
                api_tests_node --> test_screenalytics_ingest_endpoints[test_screenalytics_ingest_endpoints.py]
                api_tests_node --> test_screenalytics_runs_v2[test_screenalytics_runs_v2.py]
                api_tests_node --> test_survey_submit[test_survey_submit.py]
            end
            
            subgraph db_tests[DB]
                tests_node --> db_tests_node
                db_tests_node --> init_db_tests[__init__.py]
                db_tests_node --> test_supabase_timeout[test_supabase_timeout.py]
                db_tests_node --> test_survey_submit_rpc[test_survey_submit_rpc.sql]
            end
            
            subgraph fixtures[FIXTURES]
                tests_node --> fixtures_node
                fixtures_node --> fandom[fandom]
                fandom --> lisa_barlow_infobox[lisa_barlow_infobox.html]
                fandom --> lisa_barlow_person[lisa_barlow_person_sample.html]
                fixtures_node --> imdb[imdb]
                imdb --> episode_overview_one_season[episodes_page_overview_one_season_sample.html]
                imdb --> episode_overview[episodes_page_overview_sample.html]
                imdb --> episode_season1_next[episodes_page_season1_next_data_sample.html]
                imdb --> episode_season3[episodes_page_season3_sample.html]
                imdb --> fullcredits_cast[fullcredits_cast_sample.html]
                imdb --> list_html_fallback[list_html_fallback_sample.html]
                imdb --> list_jsonld[list_jsonld_sample.html]
                imdb --> list_sample[list_sample.html]
                imdb --> list_sample_page2[list_sample_page2.html]
                imdb --> mediaindex_tt8819906[mediaindex_tt8819906_sample.html]
                imdb --> mediaindex_viewer[mediaindex_viewer_graphql_tt8819906_sample.html]
                imdb --> person_mediaindex[person_mediaindex_nm11883948_sample.html]
                imdb --> person_mediaviewer[person_mediaviewer_nm11883948_rm1679992066_sample.html]
                imdb --> section_images[section_images_sample.html]
                imdb --> title_list_main[title_list_main_page_sample.json]
                imdb --> title_page[title_page_sample.html]
                imdb --> title_page_tt8819906[title_page_tt8819906_sample.html]
                fixtures_node --> scraping[scraping]
                scraping --> eonline_pinterest[scraping/eonline_pinterest_sample.html]
                fixtures_node --> tmdb[tmdb]
                tmdb --> find_by_imdb_id[find_by_imdb_id_sample.json]
                tmdb --> tv_alternative_titles[tv_alternative_titles_sample.json]
                tmdb --> tv_details_full[tv_details_full_sample.json]
                tmdb --> tv_details[tv_details_sample.json]
                tmdb --> tv_images[tv_images_sample.json]
                tmdb --> tv_season_details[tv_season_details_sample.json]
                tmdb --> tv_watch_providers[tv_watch_providers_sample.json]
            end
            
            subgraph ingestion[INGESTION]
                tests_node --> ingestion_node
                ingestion_node --> test_episode_appearances_upsert[test_episode_appearances_upsert.py]
                ingestion_node --> test_fandom_person_scraper[test_fandom_person_scraper.py]
                ingestion_node --> test_show_importer_metadata_enrichment[test_show_importer_metadata_enrichment.py]
                ingestion_node --> test_show_importer_tmdb_details_links_imdb_show[test_show_importer_tmdb_details_links_imdb_show.py]
                ingestion_node --> test_show_metadata_enricher[test_show_metadata_enricher.py]
                ingestion_node --> test_tmdb_show_backfill[test_tmdb_show_backfill.py]
            end
            
            subgraph integrations[INTEGRATIONS]
                tests_node --> integrations_node
                integrations_node --> fandom_tests[fandom]
                fandom_tests --> test_fandom_infobox_parser[test_fandom_infobox_parser.py]
                integrations_node --> imdb_tests[imdb]
                imdb_tests --> test_episodic_client_normalization[test_episodic_client_normalization.py]
                imdb_tests --> test_fullcredits_cast_parser[test_fullcredits_cast_parser.py]
                imdb_tests --> test_graphql_client[test_graphql_client.py]
                imdb_tests --> test_graphql_fallback_integration[test_graphql_fallback_integration.py]
                imdb_tests --> test_graphql_operations[test_graphql_operations.py]
                imdb_tests --> test_imdb_episodes_persistence[test_imdb_episodes_persistence.py]
                imdb_tests --> test_imdb_images[test_imdb_images.py]
                imdb_tests --> test_imdb_list_graphql_client_parsing[test_imdb_list_graphql_client_parsing.py]
                imdb_tests --> test_mediaindex_images[test_mediaindex_images.py]
                imdb_tests --> test_person_gallery_parser[test_person_gallery_parser.py]
                imdb_tests --> test_person_image_extraction[test_person_image_extraction.py]
                imdb_tests --> test_title_page_metadata[test_title_page_metadata.py]
                integrations_node --> tmdb_tests[tmdb]
                tmdb_tests --> test_tmdb_season_enrichment[test_tmdb_season_enrichment.py]
                tmdb_tests --> test_tmdb_tv_details_persistence[test_tmdb_tv_details_persistence.py]
                tmdb_tests --> test_tmdb_tv_images_persistence[test_tmdb_tv_images_persistence.py]
            end
            
            subgraph media_tests[MEDIA]
                tests_node --> media_tests_node
                media_tests_node --> init_media_tests[__init__.py]
                media_tests_node --> test_s3_mirror[test_s3_mirror.py]
                media_tests_node --> test_user_uploads[test_user_uploads.py]
            end
            
            subgraph migrations_tests[MIGRATIONS]
                tests_node --> migrations_tests_node
                migrations_tests_node --> test_show_source_metadata_migrations[test_show_source_metadata_migrations.py]
            end
            
            subgraph pipeline_tests[PIPELINE]
                tests_node --> pipeline_tests_node
                pipeline_tests_node --> init_pipeline_tests[__init__.py]
                pipeline_tests_node --> test_models[test_models.py]
                pipeline_tests_node --> test_orchestrator[test_orchestrator.py]
                pipeline_tests_node --> test_stages[test_stages.py]
            end
            
            subgraph repositories_tests[REPOSITORIES]
                tests_node --> repositories_tests_node
                repositories_tests_node --> test_cast_photos_upsert[test_cast_photos_upsert.py]
                repositories_tests_node --> test_credits[test_credits.py]
                repositories_tests_node --> test_credits_integration[test_credits_integration.py]
                repositories_tests_node --> test_media_assets_mirroring[test_media_assets_mirroring.py]
                repositories_tests_node --> test_media_assets_transform[test_media_assets_transform.py]
                repositories_tests_node --> test_pgrst204_retry[test_pgrst204_retry.py]
                repositories_tests_node --> test_show_images_dual_write[test_show_images_dual_write.py]
                repositories_tests_node --> test_shows_preflight[test_shows_preflight.py]
                repositories_tests_node --> test_social_season_analytics[test_social_season_analytics.py]
            end
            
            subgraph scraping_tests[SCRAPING]
                tests_node --> scraping_tests_node
                scraping_tests_node --> test_bravo_parser[test_bravo_parser.py]
                scraping_tests_node --> test_url_image_scraper[test_url_image_scraper.py]
            end
            
            tests_node --> test_api_smoke[test_api_smoke.py]
            tests_node --> test_discussions_smoke[test_discussions_smoke.py]
            tests_node --> test_dms_smoke[test_dms_smoke.py]
            tests_node --> test_fix_repo_structure_mermaid[test_fix_repo_structure_mermaid.py]
            tests_node --> test_sync_common[test_sync_common.py]
            tests_node --> test_ws_realtime_smoke[test_ws_realtime_smoke.py]
            
            subgraph utils[UTILS]
                tests_node --> utils_node
                utils_node --> test_episode_appearances_aggregation[test_episode_appearances_aggregation.py]
            end
            
            subgraph vision[VISION]
                tests_node --> vision_node
                vision_node --> test_people_count_auto_crop[test_people_count_auto_crop.py]
                vision_node --> test_text_overlay_fallback[test_text_overlay_fallback.py]
            end
        end
        
        subgraph trr_backend[TRR_BACKEND]
            root --> trr_backend_node
            trr_backend_node --> init_trr_backend[__init__.py]
            trr_backend_node --> cli[CLI]
            cli --> init_cli[__init__.py]
            cli --> main[__main__.py]
            cli --> pipeline[pipeline.py]
            
            trr_backend_node --> clients[CLIENTS]
            clients --> init_clients[__init__.py]
            clients --> screenalytics[screenalytics.py]
            
            trr_backend_node --> db[DB]
            db_node --> init_db[__init__.py]
            db_node --> admin[admin.py]
            db_node --> connection[connection.py]
            db_node --> pg[pg.py]
            db_node --> postgrest_cache[postgrest_cache.py]
            db_node --> preflight[preflight.py]
            db_node --> session[session.py]
            db_node --> show_images[show_images.py]
            
            trr_backend_node --> ingestion[INGESTION]
            ingestion_node --> init_ingestion[__init__.py]
            ingestion_node --> cast_photo_sources[cast_photo_sources.py]
            ingestion_node --> fandom_person_scraper[fandom_person_scraper.py]
            ingestion_node --> imdb_images[imdb_images.py]
            ingestion_node --> imdb_show_mediaindex[imdb_show_mediaindex.py]
            ingestion_node --> show_importer[show_importer.py]
            ingestion_node --> show_metadata_enricher[show_metadata_enricher.py]
            ingestion_node --> showinfo_overrides[showinfo_overrides.py]
            ingestion_node --> shows_from_lists[shows_from_lists.py]
            ingestion_node --> tmdb_person_images[tmdb_person_images.py]
            ingestion_node --> tmdb_show_backfill[tmdb_show_backfill.py]
            
            trr_backend_node --> integrations[INTEGRATIONS]
            integrations_node --> init_integrations[__init__.py]
            integrations_node --> fandom[fandom.py]
            integrations_node --> imdb[IMDB]
            imdb --> init_imdb[__init__.py]
            imdb --> credits_client[credits_client.py]
            imdb --> episodic_client[episodic_client.py]
            imdb --> fullcredits_cast[fullcredits_cast_parser.py]
            imdb --> graphql_operations[graphql_operations.py]
            imdb --> graphql_persisted_client[graphql_persisted_client.py]
            imdb --> list_graphql_client[list_graphql_client.py]
            imdb --> mediaindex_images[mediaindex_images.py]
            imdb --> person_gallery[person_gallery.py]
            imdb --> title_metadata_client[title_metadata_client.py]
            imdb --> title_page_metadata[title_page_metadata.py]
            integrations_node --> tmdb[tmdb]
            tmdb --> init_tmdb[__init__.py]
            tmdb --> client[client.py]
            integrations_node --> tmdb_person[tmdb_person.py]
            
            trr_backend_node --> media[MEDIA]
            media_node --> init_media[__init__.py]
            media_node --> image_variants[image_variants.py]
            media_node --> s3_mirror[s3_mirror.py]
            media_node --> user_uploads[user_uploads.py]
            
            trr_backend_node --> models[MODELS]
            models --> init_models[__init__.py]
            models --> cast_photos[cast_photos.py]
            models --> shows[shows.py]
            
            trr_backend_node --> pipeline[PIPELINE]
            pipeline --> init_pipeline[__init__.py]
            pipeline --> manifests[manifests.py]
            pipeline --> models_pipeline[models.py]
            pipeline --> orchestrator[orchestrator.py]
            pipeline --> registry[registry.py]
            pipeline --> repository[repository.py]
            
            subgraph stages[STAGES]
                pipeline --> stages_node
                stages_node --> init_stages[__init__.py]
                stages_node --> collect[collect.py]
                stages_node --> deploy[deploy.py]
                stages_node --> enrich[enrich.py]
                stages_node --> mirror[mirror.py]
                stages_node --> resolve[resolve.py]
                stages_node --> sync_screenalytics[sync_screenalytics.py]
            end
            
            trr_backend_node --> repositories[REPOSITORIES]
            repositories --> init_repositories[__init__.py]
            repositories --> cast_fandom[cast_fandom.py]
            repositories --> cast_photo_tags[cast_photo_tags.py]
            repositories --> cast_photos[cast_photos.py]
            repositories --> cast_tmdb[cast_tmdb.py]
            repositories --> credits[credits.py]
            repositories --> episode_appearances[episode_appearances.py]
            repositories --> episode_images[episode_images.py]
            repositories --> episodes[episodes.py]
            repositories --> imdb_series[imdb_series.py]
            repositories --> media_assets[media_assets.py]
            repositories --> media_links[media_links.py]
            repositories --> people[people.py]
            repositories --> person_images[person_images.py]
            repositories --> screenalytics_runs[screenalytics_runs.py]
            repositories --> season_images[season_images.py]
            repositories --> seasons[seasons.py]
            repositories --> show_cast[show_cast.py]
            repositories --> show_images[show_images.py]
            repositories --> shows[shows.py]
            repositories --> social_season_analytics[social_season_analytics.py]
            repositories --> sync_state[sync_state.py]
            repositories --> tmdb_series[tmdb_series.py]
            repositories --> web_scrape_images[web_scrape_images.py]
            
            trr_backend_node --> scraping[SCRAPING]
            scraping --> init_scraping[__init__.py]
            scraping --> bravo_parser[bravo_parser.py]
            scraping --> url_image_scraper[url_image_scraper.py]
            
            trr_backend_node --> security[SECURITY]
            security --> jwt[jwt.py]
            
            trr_backend_node --> socials[SOCIALS]
            socials_node --> init_socials[__init__.py]
            socials_node --> instagram[instagram]
            instagram --> init_instagram[__init__.py]
            instagram --> scraper_instagram[scraper.py]
            
            socials_node --> tiktok[tiktok]
            tiktok --> init_tiktok[__init__.py]
            tiktok --> scraper_tiktok[scraper.py]
            
            socials_node --> twitter[twitter]
            twitter --> init_twitter[__init__.py]
            twitter --> scraper_twitter[scraper.py]
            
            socials_node --> youtube[youtube]
            youtube --> init_youtube[__init__.py]
            youtube --> scraper_youtube[scraper.py]
            
            trr_backend_node --> utils[UTILS]
            utils_node --> init_utils[__init__.py]
            utils_node --> array_merge[array_merge.py]
            utils_node --> env[env.py]
            utils_node --> episode_appearances[episode_appearances.py]
            
            trr_backend_node --> vision[VISION]
            vision_node --> init_vision[__init__.py]
            vision_node --> text_overlay[text_overlay.py]
        end
    end
    
    style trr-backend fill:#e0f7fa,stroke:#333,stroke-width:2px;
    style claude fill:#bbdefb,stroke:#333,stroke-width:1px;
    style config fill:#bbdefb,stroke:#333,stroke-width:1px;
    style github fill:#bbdefb,stroke:#333,stroke-width:1px;
    style api fill:#bbdefb,stroke:#333,stroke-width:1px;
    style docs fill:#bbdefb,stroke:#333,stroke-width:1px;
    style scripts fill:#bbdefb,stroke:#333,stroke-width:1px;
    style supabase fill:#bbdefb,stroke:#333,stroke-width:1px;
    style tests fill:#bbdefb,stroke:#333,stroke-width:1px;
    style trr_backend fill:#bbdefb,stroke:#333,stroke-width:1px;
```