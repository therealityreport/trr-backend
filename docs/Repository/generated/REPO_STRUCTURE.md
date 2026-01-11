# Repository Structure
```markdown
trr-backend
├── .claude
│   ├── commands
│   │   ├── trr-impl.md
│   │   ├── trr-plan.md
│   │   ├── trr-pr.md
│   │   ├── trr-spec.md
│   │   ├── trr-validate.md
│   │   └── trr-wt-new.md
│   └── hooks
│       ├── before-bash.md
│       └── on-stop.md
├── .config
│   └── wt.toml
├── .env.example
├── .github
│   └── workflows
│       ├── ci.yml
│       └── repo_map.yml
├── .gitignore
├── .python-version
├── CLAUDE.md
├── Makefile
├── README.md
├── REPO_STRUCTURE.md
├── api
│   ├── __init__.py
│   ├── auth.py
│   ├── deps.py
│   ├── main.py
│   ├── realtime
│   │   ├── __init__.py
│   │   ├── broker.py
│   │   └── events.py
│   └── routers
│       ├── __init__.py
│       ├── discussions.py
│       ├── dms.py
│       ├── shows.py
│       ├── surveys.py
│       └── ws.py
├── backfill_tmdb_show_details.py
├── docs
│   ├── HISTORY_PURGE.md
│   ├── README_local.md
│   ├── Repository
│   │   ├── README.md
│   │   ├── diagrams
│   │   │   ├── git_workflow.md
│   │   │   └── system_maps.md
│   │   └── generated
│   │       ├── .gitkeep
│   │       ├── CODE_IMPORT_GRAPH.md
│   │       ├── REPO_STRUCTURE.md
│   │       ├── REPO_STRUCTURE.mermaid.md
│   │       ├── SCRIPTS_FLOW.md
│   │       └── rendered
│   │           ├── CODE_IMPORT_GRAPH-1.svg
│   │           ├── SCRIPTS_FLOW-1.svg
│   │           ├── git_workflow-1.svg
│   │           ├── system_maps-1.svg
│   │           └── system_maps-2.svg
│   ├── SHEET_EDIT_MAPPING.md
│   ├── api
│   │   └── run.md
│   ├── architecture
│   │   └── integrations.md
│   ├── architecture.md
│   ├── cloud
│   │   ├── cloud_setup.md
│   │   ├── quick_cloud_setup.md
│   │   └── setup_codespaces_credentials.md
│   ├── db
│   │   ├── commands.md
│   │   ├── schema.md
│   │   └── verification.md
│   ├── images
│   │   └── debug_imdb_credits.png
│   ├── runbooks
│   │   └── show_import_job.md
│   └── workflows
│       └── VIBE_CODING.md
├── pytest.ini
├── requirements.txt
├── resolve_tmdb_ids_via_find.py
├── ruff.toml
├── scripts
│   ├── Media
│   │   └── README.md
│   ├── README.md
│   ├── _sync_common.py
│   ├── backfill_tmdb_show_details.py
│   ├── db
│   │   ├── README.md
│   │   ├── guard_core_schema.sql
│   │   ├── reload_postgrest_schema.sql
│   │   ├── run_sql.sh
│   │   └── verify_pre_0033_cleanup.sql
│   ├── enrich_show_cast.py
│   ├── generate_repo_mermaid.py
│   ├── imdb_show_enrichment.py
│   ├── import_fandom_gallery_photos.py
│   ├── import_imdb_cast_episode_appearances.py
│   ├── import_shows_from_lists.py
│   ├── mirror_cast_photos_to_s3.py
│   ├── mirror_show_images_to_s3.py
│   ├── rebuild_hosted_urls.py
│   ├── resolve_tmdb_ids_via_find.py
│   ├── rhoslc_fandom_enrichment.py
│   ├── run_pipeline.py
│   ├── run_show_import_job.py
│   ├── supabase
│   │   └── generate_schema_docs.py
│   ├── sync_all_tables.py
│   ├── sync_cast_photos.py
│   ├── sync_episode_appearances.py
│   ├── sync_episodes.py
│   ├── sync_people.py
│   ├── sync_season_episode_images.py
│   ├── sync_seasons.py
│   ├── sync_seasons_episodes.py
│   ├── sync_show_cast.py
│   ├── sync_show_images.py
│   ├── sync_shows.py
│   ├── sync_shows_all.py
│   ├── sync_tmdb_person_images.py
│   ├── sync_tmdb_show_entities.py
│   ├── sync_tmdb_watch_providers.py
│   └── verify_schema.py
├── skills
│   └── database-designer
│       ├── SKILL.md
│       └── references
│           ├── examples.md
│           ├── playbooks.md
│           ├── repo-context.md
│           ├── templates.sql
│           └── tooling.md
├── supabase
│   ├── .gitignore
│   ├── config.toml
│   ├── migrations
│   │   ├── 0001_init.sql
│   │   ├── 0002_social.sql
│   │   ├── 0003_dms.sql
│   │   ├── 0004_core_shows.sql
│   │   ├── 0005_show_images.sql
│   │   ├── 0006_show_images_grants.sql
│   │   ├── 0007_core_shows_tmdb_id.sql
│   │   ├── 0008_show_images_tmdb_id.sql
│   │   ├── 0009_show_images_view.sql
│   │   ├── 0010_show_images_no_votes.sql
│   │   ├── 0011_show_images_view_no_votes.sql
│   │   ├── 0012_seasons_and_episodes.sql
│   │   ├── 0013_season_images.sql
│   │   ├── 0014_show_seasons_view.sql
│   │   ├── 0015_seasons_show_name.sql
│   │   ├── 0016_seasons_episode_id_arrays.sql
│   │   ├── 0017_episodes_show_name.sql
│   │   ├── 0018_imdb_cast_episode_appearances.sql
│   │   ├── 0019_imdb_cast_grants.sql
│   │   ├── 0020_reorder_show_tables.sql
│   │   ├── 0021_reorder_people_cast_seasons_episodes.sql
│   │   ├── 0022_episode_appearances_export_view.sql
│   │   ├── 0023_episode_appearances_export_view_total_episodes.sql
│   │   ├── 0024_episode_appearances_aggregate.sql
│   │   ├── 0025_sync_state.sql
│   │   ├── 0026_add_imdb_meta_to_core_shows.sql
│   │   ├── 0027_show_images_media_sources.sql
│   │   ├── 0028_normalize_shows_add_columns.sql
│   │   ├── 0029_create_source_tables.sql
│   │   ├── 0030_create_normalized_child_tables.sql
│   │   ├── 0031_update_show_images_typed.sql
│   │   ├── 0032_backfill_normalized_data.sql
│   │   ├── 0033_cleanup_legacy_jsonb_columns.sql
│   │   ├── 0034_show_images_constraints_and_show_flags.sql
│   │   ├── 0035_show_images_upsert_rpc.sql
│   │   ├── 0036_show_merge_helpers.sql
│   │   ├── 0037_collapse_show_attributes.sql
│   │   ├── 0038_update_merge_shows_arrays.sql
│   │   ├── 0039_drop_child_tables.sql
│   │   ├── 0040_create_cast_photos.sql
│   │   ├── 0041_create_cast_fandom_and_extend_cast_photos.sql
│   │   ├── 0042_revoke_cast_public_access.sql
│   │   ├── 0043_cast_photos_add_hosted_fields.sql
│   │   ├── 0044_create_cast_tmdb.sql
│   │   ├── 0045_show_images_add_hosted_fields.sql
│   │   ├── 0046_cast_photos_allow_tmdb_source.sql
│   │   ├── 0047_add_show_source_metadata.sql
│   │   ├── 0048_create_tmdb_entities_and_watch_providers.sql
│   │   ├── 0049_rename_tmdb_dimension_tables.sql
│   │   ├── 0050_drop_or_view_tmdb_imdb_series.sql
│   │   └── 0051_season_images_add_hosted_fields.sql
│   ├── schema_docs
│   │   ├── INDEX.md
│   │   ├── core.cast_fandom.json
│   │   ├── core.cast_fandom.md
│   │   ├── core.cast_memberships.json
│   │   ├── core.cast_memberships.md
│   │   ├── core.cast_photos.json
│   │   ├── core.cast_photos.md
│   │   ├── core.cast_tmdb.json
│   │   ├── core.cast_tmdb.md
│   │   ├── core.episode_appearances.json
│   │   ├── core.episode_appearances.md
│   │   ├── core.episode_cast.json
│   │   ├── core.episode_cast.md
│   │   ├── core.episodes.json
│   │   ├── core.episodes.md
│   │   ├── core.networks.json
│   │   ├── core.networks.md
│   │   ├── core.people.json
│   │   ├── core.people.md
│   │   ├── core.production_companies.json
│   │   ├── core.production_companies.md
│   │   ├── core.season_images.json
│   │   ├── core.season_images.md
│   │   ├── core.seasons.json
│   │   ├── core.seasons.md
│   │   ├── core.show_cast.json
│   │   ├── core.show_cast.md
│   │   ├── core.show_images.json
│   │   ├── core.show_images.md
│   │   ├── core.show_watch_providers.json
│   │   ├── core.show_watch_providers.md
│   │   ├── core.shows.json
│   │   ├── core.shows.md
│   │   ├── core.sync_state.json
│   │   ├── core.sync_state.md
│   │   ├── core.watch_providers.json
│   │   └── core.watch_providers.md
│   └── seed.sql
├── test_connection.py
├── tests
│   ├── __init__.py
│   ├── fixtures
│   │   ├── fandom
│   │   │   ├── lisa_barlow_infobox.html
│   │   │   └── lisa_barlow_person_sample.html
│   │   ├── imdb
│   │   │   ├── episodes_page_overview_one_season_sample.html
│   │   │   ├── episodes_page_overview_sample.html
│   │   │   ├── episodes_page_season1_next_data_sample.html
│   │   │   ├── episodes_page_season3_sample.html
│   │   │   ├── fullcredits_cast_sample.html
│   │   │   ├── list_html_fallback_sample.html
│   │   │   ├── list_jsonld_sample.html
│   │   │   ├── list_sample.html
│   │   │   ├── list_sample_page2.html
│   │   │   ├── mediaindex_tt8819906_sample.html
│   │   │   ├── mediaindex_viewer_graphql_tt8819906_sample.html
│   │   │   ├── person_mediaindex_nm11883948_sample.html
│   │   │   ├── person_mediaviewer_nm11883948_rm1679992066_sample.html
│   │   │   ├── section_images_sample.html
│   │   │   ├── title_list_main_page_sample.json
│   │   │   ├── title_page_sample.html
│   │   │   └── title_page_tt8819906_sample.html
│   │   └── tmdb
│   │       ├── find_by_imdb_id_sample.json
│   │       ├── tv_details_full_sample.json
│   │       ├── tv_details_sample.json
│   │       ├── tv_images_sample.json
│   │       ├── tv_season_details_sample.json
│   │       └── tv_watch_providers_sample.json
│   ├── ingestion
│   │   ├── test_episode_appearances_upsert.py
│   │   ├── test_fandom_person_scraper.py
│   │   ├── test_show_importer_metadata_enrichment.py
│   │   ├── test_show_importer_tmdb_details_links_imdb_show.py
│   │   ├── test_show_metadata_enricher.py
│   │   └── test_tmdb_show_backfill.py
│   ├── integrations
│   │   ├── fandom
│   │   │   └── test_fandom_infobox_parser.py
│   │   ├── imdb
│   │   │   ├── test_episodic_client_normalization.py
│   │   │   ├── test_fullcredits_cast_parser.py
│   │   │   ├── test_imdb_episodes_persistence.py
│   │   │   ├── test_imdb_images.py
│   │   │   ├── test_imdb_list_graphql_client_parsing.py
│   │   │   ├── test_mediaindex_images.py
│   │   │   ├── test_person_gallery_parser.py
│   │   │   └── test_title_page_metadata.py
│   │   └── tmdb
│   │       ├── test_tmdb_season_enrichment.py
│   │       ├── test_tmdb_tv_details_persistence.py
│   │       └── test_tmdb_tv_images_persistence.py
│   ├── media
│   │   └── test_s3_mirror.py
│   ├── migrations
│   │   └── test_show_source_metadata_migrations.py
│   ├── repositories
│   │   ├── test_cast_photos_upsert.py
│   │   ├── test_pgrst204_retry.py
│   │   └── test_shows_preflight.py
│   ├── scripts
│   │   ├── test_import_shows_from_lists_merge.py
│   │   ├── test_import_shows_from_lists_parsing.py
│   │   ├── test_import_shows_from_lists_upsert.py
│   │   ├── test_sync_incremental.py
│   │   └── test_sync_tmdb_watch_providers.py
│   ├── test_api_smoke.py
│   ├── test_discussions_smoke.py
│   ├── test_dms_smoke.py
│   ├── test_ws_realtime_smoke.py
│   └── utils
│       └── test_episode_appearances_aggregation.py
└── trr_backend
    ├── __init__.py
    ├── db
    │   ├── __init__.py
    │   ├── connection.py
    │   ├── postgrest_cache.py
    │   ├── preflight.py
    │   ├── show_images.py
    │   └── supabase.py
    ├── ingestion
    │   ├── __init__.py
    │   ├── cast_photo_sources.py
    │   ├── fandom_person_scraper.py
    │   ├── imdb_images.py
    │   ├── show_importer.py
    │   ├── show_metadata_enricher.py
    │   ├── showinfo_overrides.py
    │   ├── shows_from_lists.py
    │   ├── tmdb_person_images.py
    │   └── tmdb_show_backfill.py
    ├── integrations
    │   ├── __init__.py
    │   ├── fandom.py
    │   ├── imdb
    │   │   ├── __init__.py
    │   │   ├── credits_client.py
    │   │   ├── episodic_client.py
    │   │   ├── fullcredits_cast_parser.py
    │   │   ├── list_graphql_client.py
    │   │   ├── mediaindex_images.py
    │   │   ├── person_gallery.py
    │   │   ├── title_metadata_client.py
    │   │   └── title_page_metadata.py
    │   ├── tmdb
    │   │   ├── __init__.py
    │   │   └── client.py
    │   └── tmdb_person.py
    ├── media
    │   ├── __init__.py
    │   └── s3_mirror.py
    ├── models
    │   ├── __init__.py
    │   ├── cast_photos.py
    │   └── shows.py
    ├── repositories
    │   ├── __init__.py
    │   ├── cast_fandom.py
    │   ├── cast_photos.py
    │   ├── cast_tmdb.py
    │   ├── episode_appearances.py
    │   ├── episodes.py
    │   ├── imdb_series.py
    │   ├── people.py
    │   ├── season_images.py
    │   ├── seasons.py
    │   ├── show_cast.py
    │   ├── show_images.py
    │   ├── shows.py
    │   ├── sync_state.py
    │   └── tmdb_series.py
    └── utils
        ├── __init__.py
        ├── env.py
        └── episode_appearances.py
```
