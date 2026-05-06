"""Smoke tests for the social control-plane import surface."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import trr_backend.repositories.social_season_analytics as legacy_repo
import trr_backend.socials.account_catalog.catalog_launch as catalog_launch
import trr_backend.socials.account_catalog.catalog_progress as catalog_progress
import trr_backend.socials.account_catalog.profile_reads as profile_reads
import trr_backend.socials.analytics.read_models as analytics_read_models
import trr_backend.socials.api as socials_api
import trr_backend.socials.api.handlers as socials_api_handlers
import trr_backend.socials.api.schemas as socials_api_schemas
import trr_backend.socials.control_plane as control_plane
import trr_backend.socials.control_plane.background_tasks as background_tasks
import trr_backend.socials.control_plane.dispatch as dispatch_reads
import trr_backend.socials.control_plane.dispatch_runtime as dispatch_runtime
import trr_backend.socials.control_plane.queue_status as queue_status
import trr_backend.socials.control_plane.recovery as recovery_surface
import trr_backend.socials.control_plane.run_lifecycle as run_lifecycle
import trr_backend.socials.control_plane.run_reads as run_reads
import trr_backend.socials.control_plane.runtime as runtime_surface
import trr_backend.socials.control_plane.shared_accounts as shared_reads
import trr_backend.socials.control_plane.shared_status_reads as shared_status_reads
import trr_backend.socials.control_plane.worker_health as worker_health
import trr_backend.socials.instagram.auth_runtime as instagram_auth_runtime
import trr_backend.socials.instagram.comments_control as instagram_comments_control
import trr_backend.socials.instagram.media_mirror as instagram_media_mirror
import trr_backend.socials.instagram.persistence as instagram_persistence
import trr_backend.socials.instagram.posts_control as instagram_posts_control
import trr_backend.socials.instagram.profile_stages as instagram_profile_stages
import trr_backend.socials.pipelines as pipelines
import trr_backend.socials.pipelines.account_catalog as pipelines_account_catalog
import trr_backend.socials.pipelines.account_catalog.freshness as account_catalog_freshness
import trr_backend.socials.pipelines.account_catalog.launch as account_catalog_launch
import trr_backend.socials.pipelines.account_catalog.progress as account_catalog_progress
import trr_backend.socials.pipelines.account_catalog.review_queue as account_catalog_review_queue
import trr_backend.socials.pipelines.comments as pipelines_comments
import trr_backend.socials.pipelines.comments.instagram as comments_pipeline_instagram
import trr_backend.socials.pipelines.media_mirror as pipelines_media_mirror
import trr_backend.socials.pipelines.season_ingest as pipelines_season_ingest
import trr_backend.socials.read_models as read_models
import trr_backend.socials.read_models.account_profile as read_models_account_profile
import trr_backend.socials.read_models.account_profile.common as account_profile_common
import trr_backend.socials.read_models.account_profile.instagram as account_profile_instagram
import trr_backend.socials.read_models.coverage as read_models_coverage
import trr_backend.socials.read_models.season_analytics as read_models_season_analytics
import trr_backend.socials.social_season_analytics_impl as canonical_social_analytics

CONTROL_PLANE_DIR = Path(__file__).resolve().parents[2] / "trr_backend" / "socials" / "control_plane"
SOCIALS_DIR = Path(__file__).resolve().parents[2] / "trr_backend" / "socials"
LEGACY_COMPATIBILITY_PATH = (
    Path(__file__).resolve().parents[2] / "trr_backend" / "repositories" / "social_season_analytics.py"
)
UNIVERSAL_PACKAGE_ROOTS = (
    pipelines,
    pipelines_account_catalog,
    pipelines_comments,
    pipelines_media_mirror,
    pipelines_season_ingest,
    read_models,
    read_models_account_profile,
    read_models_coverage,
    read_models_season_analytics,
    socials_api,
    socials_api_handlers,
    socials_api_schemas,
)
PLATFORM_JOB_RUNNER_PATHS = (
    SOCIALS_DIR / "instagram" / "posts_scrapling" / "job_runner.py",
    SOCIALS_DIR / "instagram" / "comments_scrapling" / "job_runner.py",
    SOCIALS_DIR / "tiktok" / "posts_scrapling" / "job_runner.py",
    SOCIALS_DIR / "threads" / "posts_scrapling" / "job_runner.py",
)
POSTS_CATALOG_MODULES = (
    (
        "twitter",
        "trr_backend.socials.twitter.posts_catalog",
        "TwitterPostsCatalogDependencies",
        "scrape_shared_twitter_posts",
    ),
    (
        "facebook",
        "trr_backend.socials.facebook.posts_catalog",
        "FacebookPostsCatalogDependencies",
        "scrape_shared_facebook_posts",
    ),
    (
        "threads",
        "trr_backend.socials.threads.posts_catalog",
        "ThreadsPostsCatalogDependencies",
        "scrape_shared_threads_posts",
    ),
    (
        "youtube",
        "trr_backend.socials.youtube.posts_catalog",
        "YouTubePostsCatalogDependencies",
        "scrape_shared_youtube_posts",
    ),
)
LIFECYCLE_HELPER_NAMES = (
    "_new_job_progress_state",
    "_touch_job_heartbeat",
    "_emit_job_progress",
    "_finish_job",
    "_finalize_run_status",
    "_retry_backoff_seconds",
    "_now_utc",
    "_iso",
    "_metadata_dict",
)


CANONICAL_OWNER_SURFACES = (
    ("queue status", control_plane.get_queue_status, queue_status.get_queue_status, legacy_repo.get_queue_status),
    ("list runs", control_plane.list_runs, run_reads.list_runs, legacy_repo.list_runs),
    (
        "list run summaries",
        control_plane.list_run_summaries,
        run_reads.list_run_summaries,
        legacy_repo.list_run_summaries,
    ),
    (
        "run progress snapshot",
        control_plane.get_run_progress_snapshot,
        run_reads.get_run_progress_snapshot,
        legacy_repo.get_run_progress_snapshot,
    ),
    (
        "season shared status",
        control_plane.get_season_shared_status,
        shared_status_reads.get_season_shared_status,
        legacy_repo.get_season_shared_status,
    ),
    (
        "shared runs",
        control_plane.list_shared_runs,
        shared_status_reads.list_shared_runs,
        legacy_repo.list_shared_runs,
    ),
    (
        "reconcile run summaries",
        control_plane.reconcile_run_summaries,
        run_lifecycle.reconcile_run_summaries,
        legacy_repo.reconcile_run_summaries,
    ),
    (
        "claim next queued jobs",
        control_plane.claim_next_queued_jobs,
        dispatch_runtime.claim_next_queued_jobs,
        legacy_repo.claim_next_queued_jobs,
    ),
    (
        "process claimed job",
        control_plane.process_claimed_job,
        dispatch_runtime.process_claimed_job,
        legacy_repo.process_claimed_job,
    ),
    (
        "recover and dispatch due jobs",
        control_plane.recover_and_dispatch_due_social_jobs,
        dispatch_runtime.recover_and_dispatch_due_social_jobs,
        legacy_repo.recover_and_dispatch_due_social_jobs,
    ),
    (
        "catalog launch",
        control_plane.start_social_account_catalog_backfill,
        account_catalog_launch.start_social_account_catalog_backfill,
        legacy_repo.start_social_account_catalog_backfill,
    ),
    (
        "catalog progress",
        control_plane.get_social_account_catalog_run_progress,
        account_catalog_progress.get_social_account_catalog_run_progress,
        legacy_repo.get_social_account_catalog_run_progress,
    ),
    (
        "profile summary",
        control_plane.get_social_account_profile_summary,
        account_profile_common.get_social_account_profile_summary,
        legacy_repo.get_social_account_profile_summary,
    ),
    (
        "profile posts",
        control_plane.get_social_account_profile_posts,
        account_profile_common.get_social_account_profile_posts,
        legacy_repo.get_social_account_profile_posts,
    ),
    (
        "profile comments",
        control_plane.get_social_account_profile_comments,
        account_profile_common.get_social_account_profile_comments,
        legacy_repo.get_social_account_profile_comments,
    ),
    (
        "profile hashtags",
        control_plane.get_social_account_profile_hashtags,
        account_profile_common.get_social_account_profile_hashtags,
        legacy_repo.get_social_account_profile_hashtags,
    ),
    (
        "profile collaborators/tags",
        control_plane.get_social_account_profile_collaborators_tags,
        account_profile_common.get_social_account_profile_collaborators_tags,
        legacy_repo.get_social_account_profile_collaborators_tags,
    ),
    ("analytics", control_plane.get_analytics, analytics_read_models.get_analytics, legacy_repo.get_analytics),
    (
        "load instagram cookies",
        control_plane._load_instagram_cookies,
        instagram_auth_runtime._load_instagram_cookies,
        legacy_repo._load_instagram_cookies,
    ),
    (
        "batch upsert shared catalog instagram posts",
        control_plane._batch_upsert_shared_catalog_instagram_posts,
        instagram_persistence._batch_upsert_shared_catalog_instagram_posts,
        legacy_repo._batch_upsert_shared_catalog_instagram_posts,
    ),
    (
        "requeue instagram media mirror jobs",
        control_plane.requeue_instagram_media_mirror_jobs,
        instagram_media_mirror.requeue_instagram_media_mirror_jobs,
        legacy_repo.requeue_instagram_media_mirror_jobs,
    ),
    ("worker health", control_plane.get_worker_health, worker_health.get_worker_health, legacy_repo.get_worker_health),
    (
        "update worker heartbeat",
        control_plane.update_worker_heartbeat,
        worker_health.update_worker_heartbeat,
        legacy_repo.update_worker_heartbeat,
    ),
    (
        "mark worker stopped",
        control_plane.mark_worker_stopped,
        worker_health.mark_worker_stopped,
        legacy_repo.mark_worker_stopped,
    ),
    (
        "purge inactive workers",
        control_plane.purge_inactive_workers,
        worker_health.purge_inactive_workers,
        legacy_repo.purge_inactive_workers,
    ),
    (
        "worker health by lane",
        control_plane.get_worker_health_for_lane,
        worker_health.get_worker_health_for_lane,
        legacy_repo.get_worker_health_for_lane,
    ),
    (
        "load tiktok cookies",
        control_plane._load_tiktok_cookies,
        runtime_surface._load_tiktok_cookies,
        legacy_repo._load_tiktok_cookies,
    ),
)

CANONICAL_IMPL_BACKED_SURFACES = (
    (
        "catalog verification",
        control_plane.get_social_account_catalog_verification,
        shared_reads.get_social_account_catalog_verification,
        legacy_repo.get_social_account_catalog_verification,
    ),
    ("default targets", control_plane._default_targets, shared_reads._default_targets, legacy_repo._default_targets),
)


def test_control_plane_owner_map_currently_extracted_surfaces() -> None:
    for label, exported, canonical_owner, legacy_owner in CANONICAL_OWNER_SURFACES:
        assert exported is canonical_owner, label
        assert exported is not legacy_owner, label


def test_control_plane_owner_map_canonical_impl_backed_surfaces() -> None:
    for label, exported, bridge_owner, legacy_owner in CANONICAL_IMPL_BACKED_SURFACES:
        assert exported is bridge_owner, label
        assert exported is legacy_owner, label


def test_legacy_repository_path_aliases_canonical_socials_module() -> None:
    assert legacy_repo is canonical_social_analytics
    assert LEGACY_COMPATIBILITY_PATH.read_text().count("\n") <= 12


def test_posts_catalog_modules_import_before_legacy_compatibility_path() -> None:
    backend_root = Path(__file__).resolve().parents[2]
    for label, package_name, dependency_name, scrape_function_name in POSTS_CATALOG_MODULES:
        legacy_function_name = f"_scrape_shared_{label}_posts"
        code = "\n".join(
            [
                "import importlib",
                f"catalog = importlib.import_module('{package_name}.catalog')",
                f"posts_catalog = importlib.import_module('{package_name}')",
                "legacy = importlib.import_module('trr_backend.repositories.social_season_analytics')",
                "assert getattr(posts_catalog, "
                f"'{scrape_function_name}') is getattr(catalog, '{scrape_function_name}')",
                f"assert callable(getattr(catalog, '{scrape_function_name}'))",
                f"assert callable(getattr(catalog, '{dependency_name}'))",
                f"assert callable(getattr(legacy, '{legacy_function_name}'))",
            ]
        )
        result = subprocess.run(
            [sys.executable, "-c", code],
            cwd=backend_root,
            capture_output=True,
            text=True,
            check=False,
        )

        assert result.returncode == 0, f"{label}: {result.stderr}"


def test_control_plane_does_not_import_legacy_compatibility_path() -> None:
    files_with_legacy_imports: set[str] = set()
    for path in CONTROL_PLANE_DIR.glob("*.py"):
        for line in path.read_text().splitlines():
            if "trr_backend.repositories.social_season_analytics" not in line:
                continue
            if "import" not in line and "import_module" not in line:
                continue
            files_with_legacy_imports.add(path.name)
            break

    assert files_with_legacy_imports == set()


def test_universal_social_package_roots_document_ownership() -> None:
    for package in UNIVERSAL_PACKAGE_ROOTS:
        doc = package.__doc__ or ""
        assert doc.strip(), package.__name__
        assert "own" in doc.lower() or "bridge" in doc.lower(), package.__name__


def test_universal_social_package_roots_do_not_import_legacy_compatibility_path() -> None:
    package_roots = (
        SOCIALS_DIR / "pipelines",
        SOCIALS_DIR / "read_models",
        SOCIALS_DIR / "api",
    )
    files_with_legacy_imports: set[str] = set()
    for package_root in package_roots:
        for path in package_root.rglob("*.py"):
            for line in path.read_text().splitlines():
                if "trr_backend.repositories.social_season_analytics" not in line:
                    continue
                if "import" not in line and "import_module" not in line:
                    continue
                files_with_legacy_imports.add(str(path.relative_to(SOCIALS_DIR)))
                break

    assert files_with_legacy_imports == set()


def test_platform_job_runners_use_canonical_lifecycle_module_for_lifecycle_helpers() -> None:
    offenders: dict[str, list[str]] = {}
    for path in PLATFORM_JOB_RUNNER_PATHS:
        source = path.read_text()
        missing: list[str] = []
        if "trr_backend.socials.control_plane.run_lifecycle as lifecycle" not in source:
            missing.append("canonical lifecycle import")
        for helper_name in LIFECYCLE_HELPER_NAMES:
            if f"repo.{helper_name}" in source:
                missing.append(f"repo.{helper_name}")
        if missing:
            offenders[str(path.relative_to(SOCIALS_DIR))] = missing

    assert offenders == {}


def test_run_lifecycle_exposes_job_lifecycle_interface() -> None:
    for helper_name in (
        "new_job_progress_state",
        "touch_job_heartbeat",
        "emit_job_progress",
        "finish_job",
        "finalize_run_status",
        "retry_backoff_seconds",
        "now_utc",
        "format_time",
        "metadata_dict",
    ):
        assert callable(getattr(run_lifecycle, helper_name))


def test_control_plane_reexports_worker_health_surface() -> None:
    assert control_plane.get_worker_health is worker_health.get_worker_health
    assert control_plane.get_worker_health is not legacy_repo.get_worker_health
    assert control_plane.get_queue_status is worker_health.get_queue_status
    assert control_plane.get_queue_status is not legacy_repo.get_queue_status
    assert control_plane.update_worker_heartbeat is worker_health.update_worker_heartbeat
    assert control_plane.update_worker_heartbeat is not legacy_repo.update_worker_heartbeat


def test_control_plane_reexports_background_task_surface() -> None:
    assert control_plane.submit_named_background_task is background_tasks.submit_named_background_task
    assert control_plane.background_task_snapshot is background_tasks.background_task_snapshot


def test_control_plane_reexports_run_read_surface() -> None:
    assert control_plane.list_runs is dispatch_reads.list_runs
    assert control_plane.list_runs is not legacy_repo.list_runs
    assert control_plane.get_run_progress_snapshot is dispatch_reads.get_run_progress_snapshot
    assert control_plane.get_run_progress_snapshot is not legacy_repo.get_run_progress_snapshot


def test_control_plane_reexports_shared_status_read_surface() -> None:
    assert control_plane.get_season_shared_status is shared_reads.get_season_shared_status
    assert control_plane.get_season_shared_status is not legacy_repo.get_season_shared_status
    assert control_plane.list_shared_runs is shared_reads.list_shared_runs
    assert control_plane.list_shared_runs is not legacy_repo.list_shared_runs


def test_control_plane_reexports_run_lifecycle_surface() -> None:
    assert control_plane.reconcile_run_summaries is recovery_surface.reconcile_run_summaries
    assert control_plane.reconcile_run_summaries is not legacy_repo.reconcile_run_summaries


def test_control_plane_reexports_runtime_helpers() -> None:
    assert control_plane._load_instagram_cookies is instagram_auth_runtime._load_instagram_cookies
    assert control_plane._load_instagram_cookies is not legacy_repo._load_instagram_cookies
    assert control_plane._load_tiktok_cookies is runtime_surface._load_tiktok_cookies
    assert control_plane._load_tiktok_cookies is not legacy_repo._load_tiktok_cookies
    assert control_plane.SocialWorkerUnavailableError is legacy_repo.SocialWorkerUnavailableError


def test_control_plane_reexports_shared_account_surface() -> None:
    assert (
        control_plane.start_social_account_catalog_backfill
        is account_catalog_launch.start_social_account_catalog_backfill
    )
    assert (
        catalog_launch.start_social_account_catalog_backfill
        is account_catalog_launch.start_social_account_catalog_backfill
    )
    assert control_plane.start_social_account_catalog_backfill is not legacy_repo.start_social_account_catalog_backfill
    assert (
        control_plane.get_social_account_catalog_run_progress
        is account_catalog_progress.get_social_account_catalog_run_progress
    )
    assert (
        catalog_progress.get_social_account_catalog_run_progress
        is account_catalog_progress.get_social_account_catalog_run_progress
    )
    assert (
        control_plane.get_social_account_profile_summary
        is account_profile_common.get_social_account_profile_summary
    )
    assert control_plane.get_social_account_profile_posts is account_profile_common.get_social_account_profile_posts
    assert (
        control_plane.get_social_account_profile_comments
        is account_profile_common.get_social_account_profile_comments
    )
    assert (
        profile_reads.get_social_account_profile_summary
        is account_profile_common.get_social_account_profile_summary
    )
    assert profile_reads.get_social_account_profile_posts is account_profile_common.get_social_account_profile_posts
    assert (
        profile_reads.get_social_account_profile_comments
        is account_profile_common.get_social_account_profile_comments
    )
    assert control_plane.get_social_account_catalog_verification is legacy_repo.get_social_account_catalog_verification
    assert (
        control_plane._batch_upsert_shared_catalog_instagram_posts
        is instagram_persistence._batch_upsert_shared_catalog_instagram_posts
    )
    assert (
        control_plane._batch_upsert_shared_catalog_instagram_posts
        is not legacy_repo._batch_upsert_shared_catalog_instagram_posts
    )
    assert control_plane._default_targets is legacy_repo._default_targets


def test_instagram_persistence_surface_bridges_current_post_and_comment_helpers() -> None:
    assert instagram_persistence._upsert_instagram_post is not legacy_repo._upsert_instagram_post
    assert instagram_persistence._batch_upsert_shared_catalog_instagram_posts is not (
        legacy_repo._batch_upsert_shared_catalog_instagram_posts
    )
    assert instagram_persistence._upsert_instagram_comment_tree is not legacy_repo._upsert_instagram_comment_tree
    assert instagram_persistence._batch_upsert_instagram_comments is not legacy_repo._batch_upsert_instagram_comments


def test_account_catalog_package_routing_bridges() -> None:
    assert (
        pipelines_account_catalog.start_social_account_catalog_backfill
        is account_catalog_launch.start_social_account_catalog_backfill
    )
    assert (
        pipelines_account_catalog.get_social_account_catalog_run_progress
        is account_catalog_progress.get_social_account_catalog_run_progress
    )
    assert (
        pipelines_account_catalog.get_social_account_catalog_review_queue
        is account_catalog_review_queue.get_social_account_catalog_review_queue
    )
    assert (
        pipelines_account_catalog.get_social_account_catalog_freshness
        is account_catalog_freshness.get_social_account_catalog_freshness
    )
    assert (
        catalog_launch.start_social_account_catalog_backfill
        is account_catalog_launch.start_social_account_catalog_backfill
    )
    assert (
        catalog_progress.get_social_account_catalog_run_progress
        is account_catalog_progress.get_social_account_catalog_run_progress
    )


def test_account_profile_package_routing_bridges() -> None:
    assert (
        read_models_account_profile.get_social_account_profile_summary
        is account_profile_common.get_social_account_profile_summary
    )
    assert (
        read_models_account_profile.get_social_account_profile_posts
        is account_profile_common.get_social_account_profile_posts
    )
    assert (
        read_models_account_profile.get_social_account_profile_comments
        is account_profile_common.get_social_account_profile_comments
    )
    assert (
        account_profile_instagram.get_social_account_profile_summary
        is account_profile_common.get_social_account_profile_summary
    )
    assert (
        account_profile_instagram.get_social_account_profile_posts
        is account_profile_common.get_social_account_profile_posts
    )
    assert (
        account_profile_instagram.get_social_account_profile_comments
        is account_profile_common.get_social_account_profile_comments
    )
    assert (
        profile_reads.get_social_account_profile_summary
        is account_profile_common.get_social_account_profile_summary
    )
    assert profile_reads.get_social_account_profile_posts is account_profile_common.get_social_account_profile_posts
    assert (
        profile_reads.get_social_account_profile_comments
        is account_profile_common.get_social_account_profile_comments
    )


def test_instagram_room_owner_map_currently_extracted_surfaces() -> None:
    owner_surfaces = (
        (
            "comments scrape launch",
            comments_pipeline_instagram.start_social_account_comments_scrape,
            legacy_repo.start_social_account_comments_scrape,
        ),
        (
            "comments scrape preview",
            comments_pipeline_instagram.preview_social_account_comments_scrape,
            legacy_repo.preview_social_account_comments_scrape,
        ),
        (
            "comments scrape progress",
            comments_pipeline_instagram.get_social_account_comments_scrape_run_progress,
            legacy_repo.get_social_account_comments_scrape_run_progress,
        ),
        (
            "comments scrape cancel",
            comments_pipeline_instagram.cancel_social_account_comments_run,
            legacy_repo.cancel_social_account_comments_run,
        ),
        (
            "instagram posts scrape launch",
            instagram_posts_control.start_instagram_posts_scrapling_scrape,
            legacy_repo.start_instagram_posts_scrapling_scrape,
        ),
        (
            "instagram profile detail",
            instagram_profile_stages.get_instagram_profile_detail,
            legacy_repo.get_instagram_profile_detail,
        ),
        (
            "instagram profile relationships",
            instagram_profile_stages.get_instagram_profile_relationships,
            legacy_repo.get_instagram_profile_relationships,
        ),
        (
            "instagram auth repair signal",
            instagram_auth_runtime.get_instagram_auth_repair_signal,
            legacy_repo.get_instagram_auth_repair_signal,
        ),
        (
            "instagram media mirror requeue",
            instagram_media_mirror.requeue_instagram_media_mirror_jobs,
            legacy_repo.requeue_instagram_media_mirror_jobs,
        ),
    )
    for label, canonical_owner, legacy_owner in owner_surfaces:
        assert canonical_owner is not legacy_owner, label
        assert callable(legacy_owner), label

    assert (
        instagram_comments_control.start_social_account_comments_scrape
        is comments_pipeline_instagram.start_social_account_comments_scrape
    )
    assert (
        pipelines_comments.start_social_account_comments_scrape
        is comments_pipeline_instagram.start_social_account_comments_scrape
    )
