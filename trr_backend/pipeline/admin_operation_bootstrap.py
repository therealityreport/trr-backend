"""Explicit composition root for admin-operation providers."""

from __future__ import annotations

from threading import Lock

from trr_backend.pipeline.admin_operation_registry import (
    PersonImagesCapabilities,
    PersonProfileCapabilities,
    ScrapeCapabilities,
    ShowBravoCapabilities,
    ShowLinksCapabilities,
    ShowNewsCapabilities,
    ShowRolesCapabilities,
    ShowSyncCapabilities,
    late_bound_callable,
    register_admin_operation_producer,
    register_person_images_capabilities,
    register_person_profile_capabilities,
    register_scrape_capabilities,
    register_show_bravo_capabilities,
    register_show_links_capabilities,
    register_show_news_capabilities,
    register_show_roles_capabilities,
    register_show_sync_capabilities,
)

_registration_lock = Lock()
_providers_registered = False


def register_admin_operation_providers() -> None:
    """Register concrete router exports once in a deterministic assembly order."""
    global _providers_registered
    if _providers_registered:
        return
    with _registration_lock:
        if _providers_registered:
            return
        from api.routers import (
            admin_asset_batch_jobs,
            admin_bravotv_images,
            admin_person_images,
            admin_person_profile,
            admin_scrape,
            admin_show_bravo,
            admin_show_links,
            admin_show_news,
            admin_show_roles,
            admin_show_sync,
        )
        from trr_backend.repositories import reddit_refresh

        register_person_images_capabilities(
            PersonImagesCapabilities(
                _get_tmdb_id=late_bound_callable(vars(admin_person_images), "_get_tmdb_id"),
                RefreshImagesRequest=late_bound_callable(vars(admin_person_images), "RefreshImagesRequest"),
                refresh_person_images=late_bound_callable(vars(admin_person_images), "refresh_person_images"),
            )
        )
        register_person_profile_capabilities(
            PersonProfileCapabilities(
                RefreshProfileRequest=late_bound_callable(vars(admin_person_profile), "RefreshProfileRequest"),
                _run_person_profile_refresh=late_bound_callable(
                    vars(admin_person_profile), "_run_person_profile_refresh"
                ),
            )
        )
        register_scrape_capabilities(
            ScrapeCapabilities(
                ImportImageItem=late_bound_callable(vars(admin_scrape), "ImportImageItem"),
                ImportRequest=late_bound_callable(vars(admin_scrape), "ImportRequest"),
                import_images=late_bound_callable(vars(admin_scrape), "import_images"),
            )
        )
        register_show_bravo_capabilities(
            ShowBravoCapabilities(
                BravoCommitRequest=late_bound_callable(vars(admin_show_bravo), "BravoCommitRequest"),
                commit_bravo_import=late_bound_callable(vars(admin_show_bravo), "commit_bravo_import"),
                _build_show_cast_index=late_bound_callable(vars(admin_show_bravo), "_build_show_cast_index"),
                _assert_show_sync_ready_for_bravo=late_bound_callable(
                    vars(admin_show_bravo), "_assert_show_sync_ready_for_bravo"
                ),
                _persist_person_profile=late_bound_callable(vars(admin_show_bravo), "_persist_person_profile"),
                _import_bravo_person_image=late_bound_callable(vars(admin_show_bravo), "_import_bravo_person_image"),
                _extract_news_from_snapshot=late_bound_callable(vars(admin_show_bravo), "_extract_news_from_snapshot"),
            )
        )
        register_show_links_capabilities(
            ShowLinksCapabilities(
                _discover_people_links=late_bound_callable(vars(admin_show_links), "_discover_people_links"),
                _upsert_link=late_bound_callable(vars(admin_show_links), "_upsert_link"),
                _discover_show_links=late_bound_callable(vars(admin_show_links), "_discover_show_links"),
                _discover_season_links=late_bound_callable(vars(admin_show_links), "_discover_season_links"),
                _normalize_link_kind=late_bound_callable(vars(admin_show_links), "_normalize_link_kind"),
                _PERSON_SOURCE_LINK_KINDS=admin_show_links._PERSON_SOURCE_LINK_KINDS,
                _validate_person_knowledge_url=late_bound_callable(
                    vars(admin_show_links), "_validate_person_knowledge_url"
                ),
                LinkDiscoverRequest=late_bound_callable(vars(admin_show_links), "LinkDiscoverRequest"),
                _run_show_link_discovery=late_bound_callable(vars(admin_show_links), "_run_show_link_discovery"),
            )
        )
        register_show_news_capabilities(
            ShowNewsCapabilities(
                _run_google_news_sync_impl=late_bound_callable(vars(admin_show_news), "_run_google_news_sync_impl")
            )
        )
        register_show_roles_capabilities(
            ShowRolesCapabilities(
                CastMatrixSyncRequest=late_bound_callable(vars(admin_show_roles), "CastMatrixSyncRequest"),
                sync_cast_matrix_for_show=late_bound_callable(vars(admin_show_roles), "sync_cast_matrix_for_show"),
            )
        )
        register_show_sync_capabilities(
            ShowSyncCapabilities(
                _resolve_dimension_target=late_bound_callable(vars(admin_show_sync), "_resolve_dimension_target"),
                build_hosted_url=late_bound_callable(vars(admin_show_sync), "build_hosted_url"),
                _upsert_dimension_logo_asset_row=late_bound_callable(
                    vars(admin_show_sync), "_upsert_dimension_logo_asset_row"
                ),
                _set_dimension_asset_primary_flag=late_bound_callable(
                    vars(admin_show_sync), "_set_dimension_asset_primary_flag"
                ),
                _upsert_logo_import_audit=late_bound_callable(vars(admin_show_sync), "_upsert_logo_import_audit"),
                _detect_base_logo_format=late_bound_callable(vars(admin_show_sync), "_detect_base_logo_format"),
            )
        )
        register_admin_operation_producer(
            "admin_asset_batch_jobs",
            late_bound_callable(vars(admin_asset_batch_jobs), "build_batch_jobs_operation_producer"),
        )
        register_admin_operation_producer(
            "admin_scrape_import_images",
            late_bound_callable(vars(admin_scrape), "build_scrape_import_operation_producer"),
        )
        register_admin_operation_producer(
            "admin_show_links_discover",
            late_bound_callable(vars(admin_show_links), "build_show_links_discovery_operation_producer"),
        )
        register_admin_operation_producer(
            "admin_show_bravo_preview",
            late_bound_callable(vars(admin_show_bravo), "build_bravo_preview_operation_producer"),
        )
        register_admin_operation_producer(
            "admin_show_refresh",
            late_bound_callable(vars(admin_show_sync), "build_show_refresh_operation_producer"),
            accepts_operation_id=True,
        )
        register_admin_operation_producer(
            "admin_show_refresh_photos",
            late_bound_callable(vars(admin_show_sync), "build_show_refresh_photos_operation_producer"),
        )
        register_admin_operation_producer(
            "admin_person_refresh_images",
            late_bound_callable(vars(admin_person_images), "build_person_refresh_images_operation_producer"),
            accepts_operation_id=True,
        )
        register_admin_operation_producer(
            "admin_person_refresh_profile",
            late_bound_callable(vars(admin_person_profile), "build_person_refresh_profile_operation_producer"),
            accepts_operation_id=True,
        )
        register_admin_operation_producer(
            "admin_person_reprocess_images",
            late_bound_callable(vars(admin_person_images), "build_person_reprocess_images_operation_producer"),
            accepts_operation_id=True,
        )
        register_admin_operation_producer(
            reddit_refresh.REDDIT_BACKFILL_OPERATION_TYPE,
            late_bound_callable(vars(reddit_refresh), "build_reddit_refresh_backfill_operation_producer"),
            accepts_operation_id=True,
        )
        register_admin_operation_producer(
            "admin_bravotv_image_run",
            late_bound_callable(vars(admin_bravotv_images), "build_bravotv_image_operation_producer"),
        )
        _providers_registered = True
