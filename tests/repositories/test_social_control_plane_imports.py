"""Smoke tests for the social control-plane import surface."""

from __future__ import annotations

import ast
import os
import subprocess
import sys
from contextlib import nullcontext
from pathlib import Path
from textwrap import dedent
from types import SimpleNamespace

import pytest

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
import trr_backend.socials.instagram.catalog_ingest as instagram_catalog_ingest
import trr_backend.socials.instagram.comments_control as instagram_comments_control
import trr_backend.socials.instagram.media_mirror as instagram_media_mirror
import trr_backend.socials.instagram.persistence as instagram_persistence
import trr_backend.socials.instagram.posts_control as instagram_posts_control
import trr_backend.socials.instagram.profile_stages as instagram_profile_stages
import trr_backend.socials.pipelines as pipelines
import trr_backend.socials.pipelines.account_catalog as pipelines_account_catalog
import trr_backend.socials.pipelines.account_catalog.freshness as account_catalog_freshness
import trr_backend.socials.pipelines.account_catalog.launch as account_catalog_launch
import trr_backend.socials.pipelines.account_catalog.live_profile_total as account_catalog_live_total
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
ANALYTICS_CACHE_PATH = Path(__file__).resolve().parents[2] / "api" / "routers" / "socials" / "_analytics_cache.py"
SOCIALS_ROUTER_PATH = Path(__file__).resolve().parents[2] / "api" / "routers" / "socials" / "__init__.py"
ACCOUNT_CATALOG_REVIEW_QUEUE_PATH = SOCIALS_DIR / "pipelines" / "account_catalog" / "review_queue.py"
SHARED_ACCOUNTS_PATH = CONTROL_PLANE_DIR / "shared_accounts.py"
ACCOUNT_CATALOG_RUNTIME_CALLER_PATHS = (
    SOCIALS_DIR / "social_season_analytics_impl.py",
    CONTROL_PLANE_DIR / "dispatch.py",
)
ACCOUNT_CATALOG_COMPATIBILITY_MODULES = (
    "trr_backend.socials.account_catalog.catalog_launch",
    "trr_backend.socials.account_catalog.catalog_progress",
    "trr_backend.socials.account_catalog.profile_reads",
)
ACCOUNT_CATALOG_CANONICAL_MODULES = (
    "trr_backend.socials.pipelines.account_catalog.launch",
    "trr_backend.socials.pipelines.account_catalog.progress",
    "trr_backend.socials.read_models.account_profile.common",
)
RUN_READ_FACADE_IMPORT = "from trr_backend.socials.control_plane.dispatch import"
RUN_READ_CANONICAL_IMPORT = "from trr_backend.socials.control_plane.run_reads import"
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
DIRECT_SCRAPE_MODULES = (
    ("tiktok", "trr_backend.socials.tiktok.direct_scrape"),
    ("twitter", "trr_backend.socials.twitter.direct_scrape"),
    ("facebook", "trr_backend.socials.facebook.direct_scrape"),
    ("threads", "trr_backend.socials.threads.direct_scrape"),
    ("youtube", "trr_backend.socials.youtube.direct_scrape"),
)
THREADS_DIRECT_SCRAPE_PATH = SOCIALS_DIR / "threads" / "direct_scrape.py"
THREADS_SCRAPER_LEAF_MODULE = "trr_backend.socials.threads.scraper"
THREADS_MEDIA_RESOLVER_LEAF_MODULE = "trr_backend.socials.threads.media_resolver"
PLATFORM_JOB_HANDLER_REGISTRY_PATH = SOCIALS_DIR / "pipelines" / "job_handlers.py"
THREADS_COMPATIBILITY_JOBS_MODULE = "trr_backend.socials.threads.jobs"
THREADS_CANONICAL_JOB_RUNNER_MODULE = "trr_backend.socials.threads.posts_scrapling.job_runner"
TIKTOK_COMPATIBILITY_JOBS_MODULE = "trr_backend.socials.tiktok.jobs"
TIKTOK_CANONICAL_JOB_RUNNER_MODULE = "trr_backend.socials.tiktok.posts_scrapling.job_runner"
INSTAGRAM_COMPATIBILITY_JOBS_MODULE = "trr_backend.socials.instagram.jobs"
INSTAGRAM_COMMENTS_JOB_RUNNER_MODULE = "trr_backend.socials.instagram.comments_scrapling.job_runner"
INSTAGRAM_POSTS_JOB_RUNNER_MODULE = "trr_backend.socials.instagram.posts_scrapling.job_runner"
SHARED_JOB_EXECUTOR_MODULE = "trr_backend.socials.pipelines.shared_job_executor"
RUNTIME_VERSION_LEAF_MODULE = "trr_backend.runtime_version"
INSTAGRAM_POSTS_CONTROL_PATH = SOCIALS_DIR / "instagram" / "posts_control.py"
INSTAGRAM_MEDIA_MIRROR_PATH = SOCIALS_DIR / "instagram" / "media_mirror.py"
INSTAGRAM_PROFILE_STAGES_PATH = SOCIALS_DIR / "instagram" / "profile_stages.py"
INSTAGRAM_CATALOG_INGEST_PATH = SOCIALS_DIR / "instagram" / "catalog_ingest.py"
INSTAGRAM_PERSISTENCE_PATH = SOCIALS_DIR / "instagram" / "persistence.py"
INSTAGRAM_COMMENTS_PERSISTENCE_PATH = SOCIALS_DIR / "instagram" / "comments_scrapling" / "persistence.py"
INSTAGRAM_POSTS_PERSISTENCE_PATH = SOCIALS_DIR / "instagram" / "posts_scrapling" / "persistence.py"
THREADS_POSTS_PERSISTENCE_PATH = SOCIALS_DIR / "threads" / "posts_scrapling" / "persistence.py"
TIKTOK_POSTS_PERSISTENCE_PATH = SOCIALS_DIR / "tiktok" / "posts_scrapling" / "persistence.py"
TIKTOK_OPS_PATH = SOCIALS_DIR / "tiktok" / "ops.py"
COOKIE_REFRESH_OPS_PATH = SOCIALS_DIR / "ops" / "cookie_refresh.py"
PROFILE_READS_HANDLER_PATH = SOCIALS_DIR / "api" / "handlers" / "profile_reads.py"
RUNTIME_VERSION_CALLER_PATHS = (
    SOCIALS_DIR / "social_season_analytics_impl.py",
    SOCIALS_DIR.parent / "modal_dispatch.py",
)
THREADS_RUNTIME_CALLER_PATHS = (
    SOCIALS_DIR / "social_season_analytics_impl.py",
    SOCIALS_DIR / "threads" / "posts_scrapling" / "job_runner.py",
)
EXPECTED_THREADS_RUNTIME_LEAF_IMPORTS = {
    (
        SOCIALS_DIR / "social_season_analytics_impl.py",
        THREADS_SCRAPER_LEAF_MODULE,
        "_ingest_threads",
        frozenset({"ThreadsScrapeConfig", "ThreadsScraper"}),
    ),
    (
        SOCIALS_DIR / "social_season_analytics_impl.py",
        THREADS_SCRAPER_LEAF_MODULE,
        "_scrape_shared_threads_posts",
        frozenset({"ThreadsScraper"}),
    ),
    (
        SOCIALS_DIR / "social_season_analytics_impl.py",
        THREADS_SCRAPER_LEAF_MODULE,
        "_refresh_threads_post_detail_sync",
        frozenset({"ThreadsScraper"}),
    ),
    (
        SOCIALS_DIR / "social_season_analytics_impl.py",
        THREADS_SCRAPER_LEAF_MODULE,
        "refresh_post_comments",
        frozenset({"ThreadsScraper"}),
    ),
    (
        SOCIALS_DIR / "social_season_analytics_impl.py",
        THREADS_MEDIA_RESOLVER_LEAF_MODULE,
        "_run_platform_media_mirror_stage",
        frozenset({"resolve_threads_media"}),
    ),
    (
        SOCIALS_DIR / "social_season_analytics_impl.py",
        THREADS_MEDIA_RESOLVER_LEAF_MODULE,
        "_run_generic_comment_media_mirror_stage",
        frozenset({"resolve_threads_media"}),
    ),
    (
        SOCIALS_DIR / "threads" / "posts_scrapling" / "job_runner.py",
        THREADS_SCRAPER_LEAF_MODULE,
        "run_threads_posts_scrapling_job",
        frozenset({"ThreadsScrapeConfig", "ThreadsScraper"}),
    ),
}
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
RECOVERY_LEGACY_EXPORT_NAMES = (
    "cancel_active_jobs",
    "cancel_claimed_job_before_processing",
    "cancel_dispatch_blocked_jobs",
    "cancel_stuck_jobs",
    "debug_ingest_job_with_openai",
    "dismiss_recent_failures",
    "recover_stale_running_jobs",
    "reset_social_ingest_health",
)
DISPATCH_LEGACY_EXPORT_NAMES = (
    "SOCIAL_CATALOG_GAP_ANALYSIS_OPERATION_TYPE",
    "build_social_account_catalog_gap_analysis_operation_producer",
    "cancel_run",
    "ensure_media_mirror_s3_ready",
    "execute_run",
    "execute_run_with_inline_worker_registration",
    "execute_social_account_catalog_run_auth_repair",
    "ingest_season",
    "ingest_shared_accounts",
    "list_jobs",
    "orchestrate_season_ingest",
    "preview_ingest_schedule",
    "refresh_post",
    "register_week_detail_cache_invalidator",
    "request_social_account_catalog_run_auth_repair",
    "requeue_media_mirror_jobs",
    "sync_newer_social_account_catalog",
    "sync_recent_social_account_catalog",
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


def test_direct_scrape_modules_import_without_legacy_repository_path() -> None:
    backend_root = Path(__file__).resolve().parents[2]
    for label, module_name in DIRECT_SCRAPE_MODULES:
        code = "\n".join(
            [
                "import importlib",
                "import sys",
                f"module = importlib.import_module('{module_name}')",
                "assert module.__name__",
                "assert 'trr_backend.repositories.social_season_analytics' not in sys.modules",
                (
                    "assert not any(name.startswith('trr_backend.socials.threads.posts_scrapling') "
                    "for name in sys.modules)"
                    if label == "threads"
                    else "pass"
                ),
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

        module_path = SOCIALS_DIR / label / "direct_scrape.py"
        source = module_path.read_text()
        assert "trr_backend.repositories.social_season_analytics" not in source, label


def test_threads_direct_scrape_lazily_imports_canonical_scraper_leaf() -> None:
    tree = ast.parse(THREADS_DIRECT_SCRAPE_PATH.read_text())
    runtime_helpers = {
        node.name: node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name in {"scrape_threads", "preview_threads_profile"}
    }

    assert set(runtime_helpers) == {"scrape_threads", "preview_threads_profile"}
    assert not any(
        isinstance(node, ast.ImportFrom)
        and node.module == "trr_backend.socials"
        and any(alias.name == "threads" for alias in node.names)
        for node in ast.walk(tree)
    )
    assert not any(
        isinstance(node, ast.ImportFrom) and node.module == THREADS_SCRAPER_LEAF_MODULE for node in tree.body
    )

    for helper_name, helper_node in runtime_helpers.items():
        leaf_imports = [
            node
            for node in ast.walk(helper_node)
            if isinstance(node, ast.ImportFrom) and node.module == THREADS_SCRAPER_LEAF_MODULE
        ]
        assert len(leaf_imports) == 1, helper_name
        assert {alias.name for alias in leaf_imports[0].names} == {
            "ThreadsScrapeConfig",
            "ThreadsScraper",
        }


def test_threads_runtime_callers_lazily_import_only_canonical_leaf_owners() -> None:
    actual_leaf_imports: set[tuple[Path, str, str, frozenset[str]]] = set()
    package_root_imports: list[tuple[Path, int]] = []

    for path in THREADS_RUNTIME_CALLER_PATHS:
        tree = ast.parse(path.read_text())
        assert not any(
            isinstance(node, ast.ImportFrom)
            and node.module in {THREADS_SCRAPER_LEAF_MODULE, THREADS_MEDIA_RESOLVER_LEAF_MODULE}
            for node in tree.body
        ), path

        for node in ast.walk(tree):
            imports_package_root = (
                isinstance(node, ast.Import)
                and any(alias.name == "trr_backend.socials.threads" for alias in node.names)
            ) or (
                isinstance(node, ast.ImportFrom)
                and (
                    node.module == "trr_backend.socials.threads"
                    or (node.module == "trr_backend.socials" and any(alias.name == "threads" for alias in node.names))
                )
            )
            if imports_package_root:
                package_root_imports.append((path, node.lineno))

        for function_node in (node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))):
            for node in ast.walk(function_node):
                if not isinstance(node, ast.ImportFrom):
                    continue
                if node.module not in {THREADS_SCRAPER_LEAF_MODULE, THREADS_MEDIA_RESOLVER_LEAF_MODULE}:
                    continue
                actual_leaf_imports.add(
                    (
                        path,
                        node.module,
                        function_node.name,
                        frozenset(alias.name for alias in node.names),
                    )
                )

    assert package_root_imports == []
    assert actual_leaf_imports == EXPECTED_THREADS_RUNTIME_LEAF_IMPORTS


def test_central_job_handler_registry_lazily_imports_canonical_threads_runner() -> None:
    tree = ast.parse(PLATFORM_JOB_HANDLER_REGISTRY_PATH.read_text())
    registry_function = next(
        node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == "registered_platform_job_handlers"
    )

    compatibility_imports = [
        node
        for node in ast.walk(tree)
        if (
            isinstance(node, ast.Import)
            and any(alias.name == THREADS_COMPATIBILITY_JOBS_MODULE for alias in node.names)
        )
        or (isinstance(node, ast.ImportFrom) and node.module == THREADS_COMPATIBILITY_JOBS_MODULE)
    ]
    canonical_imports = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module == THREADS_CANONICAL_JOB_RUNNER_MODULE
    ]
    lazy_canonical_imports = [
        node
        for node in ast.walk(registry_function)
        if isinstance(node, ast.ImportFrom) and node.module == THREADS_CANONICAL_JOB_RUNNER_MODULE
    ]

    assert compatibility_imports == []
    assert len(canonical_imports) == 1
    assert lazy_canonical_imports == canonical_imports
    assert {alias.name for alias in canonical_imports[0].names} == {"run_threads_posts_scrapling_job"}


def test_central_job_handler_registry_lazily_imports_canonical_tiktok_runner() -> None:
    tree = ast.parse(PLATFORM_JOB_HANDLER_REGISTRY_PATH.read_text())
    registry_function = next(
        node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == "registered_platform_job_handlers"
    )

    compatibility_imports = [
        node
        for node in ast.walk(tree)
        if (
            isinstance(node, ast.Import) and any(alias.name == TIKTOK_COMPATIBILITY_JOBS_MODULE for alias in node.names)
        )
        or (isinstance(node, ast.ImportFrom) and node.module == TIKTOK_COMPATIBILITY_JOBS_MODULE)
    ]
    canonical_imports = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module == TIKTOK_CANONICAL_JOB_RUNNER_MODULE
    ]
    lazy_canonical_imports = [
        node
        for node in ast.walk(registry_function)
        if isinstance(node, ast.ImportFrom) and node.module == TIKTOK_CANONICAL_JOB_RUNNER_MODULE
    ]

    assert compatibility_imports == []
    assert len(canonical_imports) == 1
    assert lazy_canonical_imports == canonical_imports
    assert {alias.name for alias in canonical_imports[0].names} == {"run_tiktok_posts_scrapling_job"}


def test_central_job_handler_registry_lazily_imports_canonical_instagram_executors() -> None:
    tree = ast.parse(PLATFORM_JOB_HANDLER_REGISTRY_PATH.read_text())
    registry_function = next(
        node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == "registered_platform_job_handlers"
    )
    expected_imports = {
        (INSTAGRAM_COMMENTS_JOB_RUNNER_MODULE, frozenset({"run_instagram_comments_scrapling_job"})),
        (INSTAGRAM_POSTS_JOB_RUNNER_MODULE, frozenset({"run_instagram_posts_scrapling_job"})),
        (SHARED_JOB_EXECUTOR_MODULE, frozenset({"execute_shared_claimed_job"})),
    }

    compatibility_imports = [
        node
        for node in ast.walk(tree)
        if (
            isinstance(node, ast.Import)
            and any(alias.name == INSTAGRAM_COMPATIBILITY_JOBS_MODULE for alias in node.names)
        )
        or (isinstance(node, ast.ImportFrom) and node.module == INSTAGRAM_COMPATIBILITY_JOBS_MODULE)
    ]
    canonical_imports = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        and node.module
        in {
            INSTAGRAM_COMMENTS_JOB_RUNNER_MODULE,
            INSTAGRAM_POSTS_JOB_RUNNER_MODULE,
            SHARED_JOB_EXECUTOR_MODULE,
        }
    ]
    lazy_canonical_imports = [
        node
        for node in ast.walk(registry_function)
        if isinstance(node, ast.ImportFrom)
        and node.module
        in {
            INSTAGRAM_COMMENTS_JOB_RUNNER_MODULE,
            INSTAGRAM_POSTS_JOB_RUNNER_MODULE,
            SHARED_JOB_EXECUTOR_MODULE,
        }
    ]
    actual_imports = {(node.module, frozenset(alias.name for alias in node.names)) for node in canonical_imports}

    assert compatibility_imports == []
    assert len(canonical_imports) == 3
    assert lazy_canonical_imports == canonical_imports
    assert actual_imports == expected_imports


def test_runtime_version_callers_use_import_neutral_leaf_without_control_plane_cycle() -> None:
    leaf_imports_by_path: dict[Path, list[ast.ImportFrom]] = {}

    for path in RUNTIME_VERSION_CALLER_PATHS:
        tree = ast.parse(path.read_text())
        leaf_imports_by_path[path] = [
            node
            for node in tree.body
            if isinstance(node, ast.ImportFrom) and node.module == RUNTIME_VERSION_LEAF_MODULE
        ]

    for path, imports in leaf_imports_by_path.items():
        assert len(imports) == 1, path
        assert {alias.name for alias in imports[0].names} == {"build_runtime_version_stamp"}, path

    modal_dispatch_path = SOCIALS_DIR.parent / "modal_dispatch.py"
    modal_tree = ast.parse(modal_dispatch_path.read_text())
    assert not any(
        isinstance(node, ast.ImportFrom) and node.module == "trr_backend.socials.control_plane.runtime"
        for node in ast.walk(modal_tree)
    )
    heartbeat = next(
        node
        for node in modal_tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == "_record_dispatcher_heartbeat"
    )
    assert any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "_resolve_dispatcher_runtime_version_stamp"
        for node in ast.walk(heartbeat)
    )


def test_instagram_posts_control_uses_import_neutral_provider_boundary() -> None:
    tree = ast.parse(INSTAGRAM_POSTS_CONTROL_PATH.read_text())
    forbidden_modules = {
        "trr_backend.repositories.social_season_analytics",
        "trr_backend.socials.social_season_analytics_impl",
    }
    legacy_imports = [
        node
        for node in ast.walk(tree)
        if (isinstance(node, ast.ImportFrom) and node.module in forbidden_modules)
        or (isinstance(node, ast.Import) and any(alias.name in forbidden_modules for alias in node.names))
    ]

    assert legacy_imports == []
    assert any(isinstance(node, ast.FunctionDef) and node.name == "_configure_legacy_provider" for node in tree.body)
    monolith_source = (SOCIALS_DIR / "social_season_analytics_impl.py").read_text()
    assert "_configure_instagram_posts_control_legacy_provider()" in monolith_source


def test_instagram_media_mirror_uses_import_neutral_provider_boundary() -> None:
    tree = ast.parse(INSTAGRAM_MEDIA_MIRROR_PATH.read_text())
    forbidden_modules = {
        "trr_backend.repositories.social_season_analytics",
        "trr_backend.socials.social_season_analytics_impl",
    }
    legacy_imports = [
        node
        for node in ast.walk(tree)
        if (isinstance(node, ast.ImportFrom) and node.module in forbidden_modules)
        or (isinstance(node, ast.Import) and any(alias.name in forbidden_modules for alias in node.names))
    ]

    assert legacy_imports == []
    assert any(isinstance(node, ast.FunctionDef) and node.name == "_configure_legacy_provider" for node in tree.body)


def test_instagram_media_mirror_source_update_allows_missing_provider_field_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[object, ...]] = []
    provider = {
        "_instagram_posts_has_column": lambda *_args, **_kwargs: True,
        "pg": SimpleNamespace(
            db_cursor=lambda **_kwargs: nullcontext(object()),
            fetch_one_with_cursor=lambda *args: calls.append(args),
        ),
    }
    monkeypatch.setattr(instagram_media_mirror, "_LEGACY_NAMESPACE", None)
    monkeypatch.setattr(instagram_media_mirror, "_LEGACY_ORIGINALS", {})
    instagram_media_mirror._configure_legacy_provider(provider, {})

    instagram_media_mirror._update_instagram_post_source_media_fields(post_id="post-1")

    assert calls == []


def test_instagram_profile_stages_uses_import_neutral_provider_boundary() -> None:
    tree = ast.parse(INSTAGRAM_PROFILE_STAGES_PATH.read_text())
    forbidden_modules = {
        "trr_backend.repositories.social_season_analytics",
        "trr_backend.socials.social_season_analytics_impl",
    }
    legacy_imports = [
        node
        for node in ast.walk(tree)
        if (isinstance(node, ast.ImportFrom) and node.module in forbidden_modules)
        or (isinstance(node, ast.Import) and any(alias.name in forbidden_modules for alias in node.names))
    ]

    assert legacy_imports == []
    assert any(isinstance(node, ast.FunctionDef) and node.name == "_configure_legacy_provider" for node in tree.body)
    monolith_source = (SOCIALS_DIR / "social_season_analytics_impl.py").read_text()
    assert "_configure_instagram_profile_stages_legacy_provider()" in monolith_source


def test_threads_posts_persistence_uses_import_neutral_provider_boundary() -> None:
    tree = ast.parse(THREADS_POSTS_PERSISTENCE_PATH.read_text())
    forbidden_modules = {
        "trr_backend.repositories.social_season_analytics",
        "trr_backend.socials.social_season_analytics_impl",
    }
    legacy_imports = [
        node
        for node in ast.walk(tree)
        if (isinstance(node, ast.ImportFrom) and node.module in forbidden_modules)
        or (isinstance(node, ast.Import) and any(alias.name in forbidden_modules for alias in node.names))
    ]
    canonical_pg_imports = [
        node
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "trr_backend.db"
        and {alias.name for alias in node.names} == {"pg"}
    ]

    assert legacy_imports == []
    assert len(canonical_pg_imports) == 1
    assert any(isinstance(node, ast.FunctionDef) and node.name == "_configure_legacy_provider" for node in tree.body)
    monolith_source = (SOCIALS_DIR / "social_season_analytics_impl.py").read_text()
    assert "_configure_posts_persistence_legacy_providers()" in monolith_source


def test_tiktok_posts_persistence_uses_import_neutral_provider_boundary() -> None:
    tree = ast.parse(TIKTOK_POSTS_PERSISTENCE_PATH.read_text())
    forbidden_modules = {
        "trr_backend.repositories.social_season_analytics",
        "trr_backend.socials.social_season_analytics_impl",
    }
    legacy_imports = [
        node
        for node in ast.walk(tree)
        if (isinstance(node, ast.ImportFrom) and node.module in forbidden_modules)
        or (isinstance(node, ast.Import) and any(alias.name in forbidden_modules for alias in node.names))
    ]
    canonical_pg_imports = [
        node
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "trr_backend.db"
        and {alias.name for alias in node.names} == {"pg"}
    ]

    assert legacy_imports == []
    assert len(canonical_pg_imports) == 1
    assert any(isinstance(node, ast.FunctionDef) and node.name == "_configure_legacy_provider" for node in tree.body)
    monolith_source = (SOCIALS_DIR / "social_season_analytics_impl.py").read_text()
    assert "_configure_posts_persistence_legacy_providers()" in monolith_source


def test_instagram_comments_persistence_uses_import_neutral_provider_boundary() -> None:
    tree = ast.parse(INSTAGRAM_COMMENTS_PERSISTENCE_PATH.read_text())
    forbidden_modules = {
        "trr_backend.repositories.social_season_analytics",
        "trr_backend.socials.social_season_analytics_impl",
    }
    legacy_imports = [
        node
        for node in ast.walk(tree)
        if (isinstance(node, ast.ImportFrom) and node.module in forbidden_modules)
        or (isinstance(node, ast.Import) and any(alias.name in forbidden_modules for alias in node.names))
    ]
    canonical_pg_imports = [
        node
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "trr_backend.db"
        and {alias.name for alias in node.names} == {"pg"}
    ]

    assert legacy_imports == []
    assert len(canonical_pg_imports) == 1
    assert any(isinstance(node, ast.FunctionDef) and node.name == "_configure_legacy_provider" for node in tree.body)
    assert any(isinstance(node, ast.FunctionDef) and node.name == "_load_repo_helpers" for node in tree.body)
    monolith_source = (SOCIALS_DIR / "social_season_analytics_impl.py").read_text()
    assert "_configure_posts_persistence_legacy_providers()" in monolith_source


def test_instagram_persistence_uses_import_neutral_read_write_provider_boundary() -> None:
    source = INSTAGRAM_PERSISTENCE_PATH.read_text()
    tree = ast.parse(source)
    provider_class = next(
        node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "_LegacyProviderProxy"
    )
    provider_methods = {node.name for node in provider_class.body if isinstance(node, ast.FunctionDef)}
    loader = next(
        node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "_catalog_ingest_module"
    )
    top_level_catalog_imports = [
        node
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "trr_backend.socials.instagram"
        and any(alias.name == "catalog_ingest" for alias in node.names)
    ]
    lazy_catalog_imports = [
        node
        for node in ast.walk(loader)
        if isinstance(node, ast.ImportFrom)
        and node.module == "trr_backend.socials.instagram"
        and {alias.name for alias in node.names} == {"catalog_ingest"}
    ]

    assert "trr_backend.repositories.social_season_analytics" not in source
    assert "trr_backend.socials.social_season_analytics_impl" not in source
    assert {"__getattr__", "__setattr__"} <= provider_methods
    assert any(isinstance(node, ast.FunctionDef) and node.name == "_configure_legacy_provider" for node in tree.body)
    assert top_level_catalog_imports == []
    assert len(lazy_catalog_imports) == 1

    monolith_source = (SOCIALS_DIR / "social_season_analytics_impl.py").read_text()
    assert "from trr_backend.socials.instagram import persistence as instagram_persistence" in monolith_source
    assert (
        "for provider in (instagram_persistence, comments_persistence, threads_persistence, tiktok_persistence):"
    ) in monolith_source


def test_instagram_posts_persistence_uses_import_neutral_shared_provider_boundary() -> None:
    source = INSTAGRAM_POSTS_PERSISTENCE_PATH.read_text()
    tree = ast.parse(source)
    persist_function = next(
        node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "persist_instagram_posts"
    )
    provider_imports = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        and node.module == "trr_backend.socials.instagram.comments_scrapling.persistence"
        and {alias.name for alias in node.names} == {"_load_repo_helpers"}
    ]
    provider_bindings = [
        node
        for node in ast.walk(persist_function)
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "repo" for target in node.targets)
        and isinstance(node.value, ast.Call)
        and isinstance(node.value.func, ast.Name)
        and node.value.func.id == "_load_repo_helpers"
    ]

    assert "trr_backend.repositories.social_season_analytics" not in source
    assert "trr_backend.socials.social_season_analytics_impl" not in source
    assert "repo.__dict__" not in source
    assert len(provider_imports) == 1
    assert len(provider_bindings) == 1


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


def test_recovery_dispatch_and_runtime_surfaces_reuse_existing_exact_module_proxies() -> None:
    def assert_surface(filename: str, proxy_module: str, expected_names: tuple[str, ...]) -> None:
        source = (CONTROL_PLANE_DIR / filename).read_text()
        tree = ast.parse(source)
        proxy_imports = [
            alias
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.module == proxy_module
            for alias in node.names
            if alias.name == "legacy" and alias.asname == "_legacy"
        ]
        proxy_bindings = {
            target.id: node.value.attr
            for node in tree.body
            if isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance((target := node.targets[0]), ast.Name)
            and isinstance(node.value, ast.Attribute)
            and isinstance(node.value.value, ast.Name)
            and node.value.value.id == "_legacy"
        }
        deleted_names = {
            target.id
            for node in tree.body
            if isinstance(node, ast.Delete)
            for target in node.targets
            if isinstance(target, ast.Name)
        }

        assert "trr_backend.repositories.social_season_analytics" not in source
        assert "trr_backend.socials.social_season_analytics_impl" not in source
        assert len(proxy_imports) == 1
        assert proxy_bindings == {name: name for name in expected_names}
        assert "_legacy" in deleted_names

    assert_surface(
        "recovery.py",
        "trr_backend.socials.control_plane.run_lifecycle",
        RECOVERY_LEGACY_EXPORT_NAMES,
    )
    assert_surface(
        "dispatch.py",
        "trr_backend.socials.control_plane.dispatch_runtime",
        DISPATCH_LEGACY_EXPORT_NAMES,
    )

    runtime_source = (CONTROL_PLANE_DIR / "runtime.py").read_text()
    runtime_tree = ast.parse(runtime_source)
    runtime_proxy_imports = [
        alias
        for node in ast.walk(runtime_tree)
        if isinstance(node, ast.ImportFrom) and node.module == "trr_backend.socials.control_plane.dispatch_runtime"
        for alias in node.names
        if alias.name == "legacy" and alias.asname == "_core"
    ]
    runtime_deleted_names = {
        target.id
        for node in runtime_tree.body
        if isinstance(node, ast.Delete)
        for target in node.targets
        if isinstance(target, ast.Name)
    }
    runtime_core_references = [
        node
        for node in ast.walk(runtime_tree)
        if isinstance(node, ast.Name) and node.id == "_core" and isinstance(node.ctx, ast.Load)
    ]

    assert "trr_backend.repositories.social_season_analytics" not in runtime_source
    assert "trr_backend.socials.social_season_analytics_impl" not in runtime_source
    assert len(runtime_proxy_imports) == 1
    assert runtime_core_references
    assert "_core" not in runtime_deleted_names


def test_dispatch_runtime_reuses_run_lifecycle_exact_loader_without_legacy_import() -> None:
    source = (CONTROL_PLANE_DIR / "dispatch_runtime.py").read_text()
    tree = ast.parse(source)
    forbidden_modules = {
        "trr_backend.repositories.social_season_analytics",
        "trr_backend.socials.social_season_analytics_impl",
    }
    direct_legacy_imports = [
        node
        for node in ast.walk(tree)
        if (isinstance(node, ast.ImportFrom) and node.module in forbidden_modules)
        or (isinstance(node, ast.Import) and any(alias.name in forbidden_modules for alias in node.names))
    ]
    provider_imports = [
        node
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "trr_backend.socials.control_plane.run_lifecycle"
        and [(alias.name, alias.asname) for alias in node.names] == [("_legacy_module", "_load_legacy_module")]
    ]
    provider_assignments = [
        node
        for node in tree.body
        if isinstance(node, ast.Assign)
        and [target.id for target in node.targets if isinstance(target, ast.Name)] == ["legacy"]
        and isinstance(node.value, ast.Call)
        and isinstance(node.value.func, ast.Name)
        and node.value.func.id == "_load_legacy_module"
        and not node.value.args
        and not node.value.keywords
    ]
    deleted_names = {
        target.id
        for node in tree.body
        if isinstance(node, ast.Delete)
        for target in node.targets
        if isinstance(target, ast.Name)
    }

    assert direct_legacy_imports == []
    assert len(provider_imports) == 1
    assert len(provider_assignments) == 1
    assert "_load_legacy_module" in deleted_names
    assert "_load_legacy_module" not in dispatch_runtime.__dict__
    assert dispatch_runtime.legacy is canonical_social_analytics
    assert run_lifecycle._legacy_module() is canonical_social_analytics


def test_instagram_catalog_ingest_reuses_run_lifecycle_exact_loader_without_legacy_import() -> None:
    source = INSTAGRAM_CATALOG_INGEST_PATH.read_text()
    tree = ast.parse(source)
    forbidden_modules = {
        "trr_backend.repositories.social_season_analytics",
        "trr_backend.socials.social_season_analytics_impl",
    }
    direct_legacy_imports = [
        node
        for node in ast.walk(tree)
        if (isinstance(node, ast.ImportFrom) and node.module in forbidden_modules)
        or (isinstance(node, ast.Import) and any(alias.name in forbidden_modules for alias in node.names))
    ]
    provider_imports = [
        node
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "trr_backend.socials.control_plane.run_lifecycle"
        and [(alias.name, alias.asname) for alias in node.names] == [("_legacy_module", "_load_legacy_module")]
    ]
    provider_assignments = [
        node
        for node in tree.body
        if isinstance(node, ast.Assign)
        and [target.id for target in node.targets if isinstance(target, ast.Name)] == ["_core"]
        and isinstance(node.value, ast.Call)
        and isinstance(node.value.func, ast.Name)
        and node.value.func.id == "_load_legacy_module"
        and not node.value.args
        and not node.value.keywords
    ]
    deleted_names = {
        target.id
        for node in tree.body
        if isinstance(node, ast.Delete)
        for target in node.targets
        if isinstance(target, ast.Name)
    }

    assert direct_legacy_imports == []
    assert len(provider_imports) == 1
    assert len(provider_assignments) == 1
    assert "_load_legacy_module" in deleted_names
    assert "_load_legacy_module" not in instagram_catalog_ingest.__dict__
    assert instagram_catalog_ingest._core is canonical_social_analytics
    assert run_lifecycle._legacy_module() is canonical_social_analytics


def test_analytics_cache_registration_reuses_live_dispatch_runtime_proxy() -> None:
    source = ANALYTICS_CACHE_PATH.read_text()
    tree = ast.parse(source)
    proxy_imports = [
        alias
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module == "trr_backend.socials.control_plane.dispatch_runtime"
        for alias in node.names
        if alias.name == "legacy" and alias.asname == "social_repo"
    ]
    registration_calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "social_repo"
        and node.func.attr == "register_week_detail_cache_invalidator"
    ]

    assert "trr_backend.repositories.social_season_analytics" not in source
    assert "trr_backend.socials.social_season_analytics_impl" not in source
    assert len(proxy_imports) == 1
    assert len(registration_calls) == 1


def test_account_catalog_review_queue_reuses_dispatch_runtime_proxy_without_legacy_import() -> None:
    source = ACCOUNT_CATALOG_REVIEW_QUEUE_PATH.read_text()
    tree = ast.parse(source)
    proxy_imports = [
        alias
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module == "trr_backend.socials.control_plane.dispatch_runtime"
        for alias in node.names
        if alias.name == "legacy" and alias.asname == "_core"
    ]
    copied_bindings = {
        target.id: node.value.attr
        for node in tree.body
        if isinstance(node, ast.Assign)
        and isinstance(node.value, ast.Attribute)
        and isinstance(node.value.value, ast.Name)
        and node.value.value.id == "_core"
        for target in node.targets
        if isinstance(target, ast.Name)
    }

    assert "trr_backend.repositories.social_season_analytics" not in source
    assert "trr_backend.socials.social_season_analytics_impl" not in source
    assert len(proxy_imports) == 1
    assert copied_bindings == {
        "get_social_account_catalog_review_queue": "get_social_account_catalog_review_queue",
        "resolve_social_account_catalog_review_queue_item": ("resolve_social_account_catalog_review_queue_item"),
    }


def test_shared_accounts_reuses_import_time_dispatch_runtime_copies_without_legacy_import() -> None:
    source = SHARED_ACCOUNTS_PATH.read_text()
    tree = ast.parse(source)
    proxy_imports = [
        alias
        for node in tree.body
        if isinstance(node, ast.ImportFrom) and node.module == "trr_backend.socials.control_plane.dispatch_runtime"
        for alias in node.names
        if alias.name == "legacy" and alias.asname == "_legacy"
    ]
    copied_bindings = {
        target.id: node.value.attr
        for node in tree.body
        if isinstance(node, ast.Assign)
        and len(node.targets) == 1
        and isinstance((target := node.targets[0]), ast.Name)
        and isinstance(node.value, ast.Attribute)
        and isinstance(node.value.value, ast.Name)
        and node.value.value.id == "_legacy"
    }
    deleted_names = {
        target.id
        for node in tree.body
        if isinstance(node, ast.Delete)
        for target in node.targets
        if isinstance(target, ast.Name)
    }
    expected_names = {
        "_default_targets",
        "_normalize_catalog_backfill_window",
        "_shared_account_catalog_requires_modal_executor",
        "cancel_shared_run",
        "dismiss_social_account_catalog_run",
        "get_season_context",
        "get_shared_account_sources",
        "get_social_account_catalog_freshness",
        "get_social_account_catalog_gap_analysis_status",
        "get_social_account_catalog_posts",
        "get_social_account_catalog_review_queue",
        "get_social_account_catalog_verification",
        "get_social_account_profile_hashtag_timeline",
        "get_targets",
        "list_shared_review_queue",
        "put_shared_account_sources",
        "put_social_account_profile_hashtags",
        "put_targets",
        "resolve_shared_review_queue_item",
        "resolve_social_account_catalog_review_queue_item",
    }

    assert "trr_backend.repositories.social_season_analytics" not in source
    assert "trr_backend.socials.social_season_analytics_impl" not in source
    assert len(proxy_imports) == 1
    assert copied_bindings == {
        **{name: name for name in expected_names},
        "_legacy_cancel_social_account_catalog_run": "cancel_social_account_catalog_run",
    }
    assert "_legacy" in deleted_names


def test_tiktok_ops_reuses_lazy_dispatch_runtime_proxy_without_legacy_import() -> None:
    source = TIKTOK_OPS_PATH.read_text()
    tree = ast.parse(source)
    smoke_function = next(
        node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "run_posts_scrapling_smoke"
    )
    proxy_imports = [
        alias
        for node in ast.walk(smoke_function)
        if isinstance(node, ast.ImportFrom) and node.module == "trr_backend.socials.control_plane.dispatch_runtime"
        for alias in node.names
        if alias.name == "legacy" and alias.asname == "repo"
    ]

    assert "trr_backend.repositories.social_season_analytics" not in source
    assert "trr_backend.socials.social_season_analytics_impl" not in source
    assert len(proxy_imports) == 1


def test_cookie_refresh_ops_reuses_dispatch_runtime_proxy_without_legacy_import() -> None:
    source = COOKIE_REFRESH_OPS_PATH.read_text()
    tree = ast.parse(source)
    proxy_imports = [
        alias
        for node in tree.body
        if isinstance(node, ast.ImportFrom) and node.module == "trr_backend.socials.control_plane.dispatch_runtime"
        for alias in node.names
        if alias.name == "legacy" and alias.asname == "social_repo"
    ]

    assert "trr_backend.repositories.social_season_analytics" not in source
    assert "trr_backend.socials.social_season_analytics_impl" not in source
    assert len(proxy_imports) == 1


def test_profile_reads_reuses_function_scoped_dispatch_runtime_proxy_without_legacy_import() -> None:
    source = PROFILE_READS_HANDLER_PATH.read_text()
    tree = ast.parse(source)
    proxy_imports = [
        (node, alias)
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module == "trr_backend.socials.control_plane.dispatch_runtime"
        for alias in node.names
        if alias.name == "legacy" and alias.asname == "social_core"
    ]
    module_level_imports = [
        node
        for node in tree.body
        if isinstance(node, ast.ImportFrom) and node.module == "trr_backend.socials.control_plane.dispatch_runtime"
    ]

    assert "trr_backend.repositories.social_season_analytics" not in source
    assert "trr_backend.socials.social_season_analytics_impl" not in source
    assert len(proxy_imports) == 15
    assert module_level_imports == []


def test_queue_status_does_not_import_either_legacy_social_module() -> None:
    source = (CONTROL_PLANE_DIR / "queue_status.py").read_text()

    assert "trr_backend.repositories.social_season_analytics" not in source
    assert "trr_backend.socials.social_season_analytics_impl" not in source
    assert "import_module" not in source


def test_social_health_dot_reuses_queue_status_provider_without_legacy_import() -> None:
    tree = ast.parse(SOCIALS_ROUTER_PATH.read_text())
    functions = [
        node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "get_social_ingest_health_dot"
    ]
    assert len(functions) == 1
    function = functions[0]
    forbidden_modules = {
        "trr_backend.repositories.social_season_analytics",
        "trr_backend.socials.social_season_analytics_impl",
    }
    legacy_imports = [
        node
        for node in ast.walk(function)
        if (isinstance(node, ast.ImportFrom) and node.module in forbidden_modules)
        or (isinstance(node, ast.Import) and any(alias.name in forbidden_modules for alias in node.names))
    ]
    provider_imports = [
        node
        for node in ast.walk(function)
        if isinstance(node, ast.ImportFrom)
        and node.module == "trr_backend.socials.control_plane.queue_status"
        and [(alias.name, alias.asname) for alias in node.names] == [("_legacy_repo", None)]
    ]
    provider_calls = [
        node
        for node in ast.walk(function)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "get_queue_status"
        and isinstance(node.func.value, ast.Call)
        and isinstance(node.func.value.func, ast.Name)
        and node.func.value.func.id == "_legacy_repo"
        and not node.func.value.args
        and not node.func.value.keywords
    ]

    assert legacy_imports == []
    assert len(provider_imports) == 1
    assert len(provider_calls) == 1
    assert [keyword.arg for keyword in provider_calls[0].keywords] == [
        "include_recent_failures",
        "include_stuck_jobs",
        "include_runs_summary",
        "summary_only",
        "statement_timeout_ms",
    ]


def test_social_queue_status_reuses_queue_status_provider_without_legacy_import() -> None:
    tree = ast.parse(SOCIALS_ROUTER_PATH.read_text())
    functions = [
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "get_social_ingest_queue_status"
    ]
    assert len(functions) == 1
    function = functions[0]
    forbidden_modules = {
        "trr_backend.repositories.social_season_analytics",
        "trr_backend.socials.social_season_analytics_impl",
    }
    legacy_imports = [
        node
        for node in ast.walk(function)
        if (isinstance(node, ast.ImportFrom) and node.module in forbidden_modules)
        or (isinstance(node, ast.Import) and any(alias.name in forbidden_modules for alias in node.names))
    ]
    provider_imports = [
        node
        for node in ast.walk(function)
        if isinstance(node, ast.ImportFrom)
        and node.module == "trr_backend.socials.control_plane.queue_status"
        and [(alias.name, alias.asname) for alias in node.names] == [("_legacy_repo", None)]
    ]
    provider_calls = [
        node
        for node in ast.walk(function)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "get_queue_status"
        and isinstance(node.func.value, ast.Call)
        and isinstance(node.func.value.func, ast.Name)
        and node.func.value.func.id == "_legacy_repo"
        and not node.func.value.args
        and not node.func.value.keywords
    ]

    assert legacy_imports == []
    assert len(provider_imports) == 1
    assert len(provider_calls) == 1
    assert [keyword.arg for keyword in provider_calls[0].keywords] == [
        "fresh",
        "include_recent_failures",
        "include_stuck_jobs",
        "include_runs_summary",
        "summary_only",
        "statement_timeout_ms",
    ]


def test_social_worker_detail_reuses_queue_status_provider_without_legacy_import() -> None:
    source = SOCIALS_ROUTER_PATH.read_text()
    tree = ast.parse(source)
    functions = [
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "get_social_ingest_worker_detail"
    ]
    assert len(functions) == 1
    function = functions[0]
    forbidden_modules = {
        "trr_backend.repositories.social_season_analytics",
        "trr_backend.socials.social_season_analytics_impl",
    }
    function_source = ast.get_source_segment(source, function) or ""
    legacy_imports = [
        node
        for node in ast.walk(function)
        if (isinstance(node, ast.ImportFrom) and node.module in forbidden_modules)
        or (isinstance(node, ast.Import) and any(alias.name in forbidden_modules for alias in node.names))
    ]
    provider_imports = [
        node
        for node in ast.walk(function)
        if isinstance(node, ast.ImportFrom)
        and node.module == "trr_backend.socials.control_plane.queue_status"
        and [(alias.name, alias.asname) for alias in node.names] == [("_legacy_repo", None)]
    ]
    provider_calls = [
        node
        for node in ast.walk(function)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "get_worker_detail"
        and isinstance(node.func.value, ast.Call)
        and isinstance(node.func.value.func, ast.Name)
        and node.func.value.func.id == "_legacy_repo"
        and not node.func.value.args
        and not node.func.value.keywords
    ]

    assert legacy_imports == []
    assert all(module not in function_source for module in forbidden_modules)
    assert len(provider_imports) == 1
    assert len(provider_calls) == 1
    assert len(provider_calls[0].args) == 1
    assert isinstance(provider_calls[0].args[0], ast.Name)
    assert provider_calls[0].args[0].id == "worker_id"
    assert provider_calls[0].keywords == []


def test_social_purge_inactive_workers_reuses_queue_status_provider_without_legacy_import() -> None:
    source = SOCIALS_ROUTER_PATH.read_text()
    tree = ast.parse(source)
    functions = [
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "purge_social_ingest_inactive_workers"
    ]
    assert len(functions) == 1
    function = functions[0]
    forbidden_modules = {
        "trr_backend.repositories.social_season_analytics",
        "trr_backend.socials.social_season_analytics_impl",
    }
    function_source = ast.get_source_segment(source, function) or ""
    legacy_imports = [
        node
        for node in ast.walk(function)
        if (isinstance(node, ast.ImportFrom) and node.module in forbidden_modules)
        or (isinstance(node, ast.Import) and any(alias.name in forbidden_modules for alias in node.names))
    ]
    provider_imports = [
        node
        for node in ast.walk(function)
        if isinstance(node, ast.ImportFrom)
        and node.module == "trr_backend.socials.control_plane.queue_status"
        and [(alias.name, alias.asname) for alias in node.names] == [("_legacy_repo", "_repo")]
    ]
    provider_calls = [
        node
        for node in ast.walk(function)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "purge_inactive_workers"
        and isinstance(node.func.value, ast.Call)
        and isinstance(node.func.value.func, ast.Name)
        and node.func.value.func.id == "_repo"
        and not node.func.value.args
        and not node.func.value.keywords
    ]

    assert legacy_imports == []
    assert all(module not in function_source for module in forbidden_modules)
    assert len(provider_imports) == 1
    assert len(provider_calls) == 1
    assert provider_calls[0].args == []
    assert [keyword.arg for keyword in provider_calls[0].keywords] == ["stale_after_seconds"]


def test_worker_health_reuses_queue_status_provider_without_legacy_import() -> None:
    source = (CONTROL_PLANE_DIR / "worker_health.py").read_text()

    assert "trr_backend.repositories.social_season_analytics" not in source
    assert "trr_backend.socials.social_season_analytics_impl" not in source
    assert "from trr_backend.socials.control_plane.queue_status import _legacy_repo, get_queue_status" in source
    assert "_core = _legacy_repo()" in source


def test_backfill_health_reuses_queue_status_provider_without_legacy_import() -> None:
    source = (CONTROL_PLANE_DIR / "backfill_health.py").read_text()

    assert "trr_backend.repositories.social_season_analytics" not in source
    assert "trr_backend.socials.social_season_analytics_impl" not in source
    assert "from trr_backend.socials.control_plane.queue_status import _legacy_repo, get_queue_status" in source
    assert "_core = _legacy_repo()" in source
    assert "_core.__dict__" not in source


def test_shared_status_reads_reuses_queue_status_provider_without_legacy_import() -> None:
    source = (CONTROL_PLANE_DIR / "shared_status_reads.py").read_text()

    assert "trr_backend.repositories.social_season_analytics" not in source
    assert "trr_backend.socials.social_season_analytics_impl" not in source
    assert "from trr_backend.socials.control_plane.queue_status import _legacy_repo" in source
    assert "legacy = _legacy_repo()" in source
    assert "legacy.__dict__" not in source


def test_run_reads_reuses_queue_status_provider_without_legacy_import() -> None:
    source = (CONTROL_PLANE_DIR / "run_reads.py").read_text()

    assert "trr_backend.repositories.social_season_analytics" not in source
    assert "trr_backend.socials.social_season_analytics_impl" not in source
    assert "from trr_backend.socials.control_plane.queue_status import _legacy_repo" in source
    assert "legacy = _legacy_repo()" in source
    assert "legacy.__dict__" not in source


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


def test_instagram_posts_job_runner_lazily_uses_exact_dispatch_runtime_proxy() -> None:
    path = SOCIALS_DIR / "instagram" / "posts_scrapling" / "job_runner.py"
    source = path.read_text()
    tree = ast.parse(source)
    runner = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "run_instagram_posts_scrapling_job"
    )
    boundary_imports = [
        node
        for node in ast.walk(runner)
        if isinstance(node, ast.ImportFrom) and node.module == "trr_backend.socials.control_plane.dispatch_runtime"
    ]

    assert len(boundary_imports) == 1
    assert runner.body[0] is boundary_imports[0]
    assert [(alias.name, alias.asname) for alias in boundary_imports[0].names] == [("legacy", "repo")]
    assert "trr_backend.repositories.social_season_analytics" not in source
    assert "trr_backend.socials.social_season_analytics_impl" not in source


def test_instagram_comments_job_runner_lazily_uses_exact_dispatch_runtime_proxy() -> None:
    path = SOCIALS_DIR / "instagram" / "comments_scrapling" / "job_runner.py"
    source = path.read_text()
    tree = ast.parse(source)
    runner = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "run_instagram_comments_scrapling_job"
    )
    boundary_imports = [
        node
        for node in ast.walk(runner)
        if isinstance(node, ast.ImportFrom) and node.module == "trr_backend.socials.control_plane.dispatch_runtime"
    ]

    assert len(boundary_imports) == 1
    assert runner.body[0] is boundary_imports[0]
    assert [(alias.name, alias.asname) for alias in boundary_imports[0].names] == [("legacy", "repo")]
    assert "trr_backend.repositories.social_season_analytics" not in source
    assert "trr_backend.socials.social_season_analytics_impl" not in source


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
    assert control_plane.get_social_account_profile_summary is account_profile_common.get_social_account_profile_summary
    assert control_plane.get_social_account_profile_posts is account_profile_common.get_social_account_profile_posts
    assert (
        control_plane.get_social_account_profile_comments is account_profile_common.get_social_account_profile_comments
    )
    assert profile_reads.get_social_account_profile_summary is account_profile_common.get_social_account_profile_summary
    assert profile_reads.get_social_account_profile_posts is account_profile_common.get_social_account_profile_posts
    assert (
        profile_reads.get_social_account_profile_comments is account_profile_common.get_social_account_profile_comments
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


def test_account_catalog_runtime_callers_use_canonical_leaf_owners() -> None:
    source = "\n".join(path.read_text() for path in ACCOUNT_CATALOG_RUNTIME_CALLER_PATHS)

    for compatibility_module in ACCOUNT_CATALOG_COMPATIBILITY_MODULES:
        assert compatibility_module not in source
    for canonical_module in ACCOUNT_CATALOG_CANONICAL_MODULES:
        assert canonical_module in source


@pytest.mark.parametrize(
    ("leaf_module", "function_name"),
    (
        (account_catalog_launch, "start_social_account_catalog_backfill"),
        (account_catalog_launch, "begin_social_account_catalog_backfill_launch"),
        (account_catalog_launch, "finalize_social_account_catalog_backfill_launch"),
        (account_catalog_launch, "launch_social_account_catalog_backfill"),
        (account_catalog_progress, "get_social_account_catalog_run_progress"),
        (account_profile_common, "get_social_account_profile_summary"),
        (account_profile_common, "get_social_account_profile_posts"),
        (account_profile_common, "get_social_account_profile_comments"),
        (account_profile_common, "get_social_account_profile_hashtags"),
        (account_profile_common, "get_social_account_profile_collaborators_tags"),
    ),
)
def test_legacy_account_catalog_wrappers_delegate_to_canonical_leaf_rooms(
    monkeypatch: pytest.MonkeyPatch,
    leaf_module: object,
    function_name: str,
) -> None:
    captured: dict[str, object] = {}
    expected = object()

    def replacement(*args: object, **kwargs: object) -> object:
        captured["args"] = args
        captured["kwargs"] = kwargs
        return expected

    monkeypatch.setitem(leaf_module._LOCAL_ROOM_FUNCTIONS, function_name, replacement)  # type: ignore[attr-defined]

    result = getattr(canonical_social_analytics, function_name)("sentinel", marker=True)

    assert result is expected
    assert captured == {"args": ("sentinel",), "kwargs": {"marker": True}}


def test_legacy_run_read_callers_use_canonical_leaf_owner() -> None:
    source = (SOCIALS_DIR / "social_season_analytics_impl.py").read_text()

    assert RUN_READ_FACADE_IMPORT not in source
    assert source.count(RUN_READ_CANONICAL_IMPORT) == 3


@pytest.mark.parametrize(
    ("function_name", "args", "kwargs"),
    (
        ("list_runs", ("season-1",), {"limit": 7}),
        ("list_run_summaries", ("season-1",), {"limit": 7}),
        ("get_run_progress_snapshot", ("season-1", "run-1"), {"recent_log_limit": 7}),
    ),
)
def test_legacy_run_read_wrappers_delegate_to_canonical_leaf(
    monkeypatch: pytest.MonkeyPatch,
    function_name: str,
    args: tuple[str, ...],
    kwargs: dict[str, int],
) -> None:
    captured: dict[str, object] = {}
    expected = object()

    def replacement(*call_args: object, **call_kwargs: object) -> object:
        captured["args"] = call_args
        captured["kwargs"] = call_kwargs
        return expected

    monkeypatch.setattr(run_reads, function_name, replacement)

    result = getattr(canonical_social_analytics, function_name)(*args, **kwargs)

    assert result is expected
    assert captured["args"] == args
    captured_kwargs = captured["kwargs"]
    assert isinstance(captured_kwargs, dict)
    assert {key: captured_kwargs[key] for key in kwargs} == kwargs


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
        legacy_repo.get_social_account_catalog_freshness
        is account_catalog_freshness.get_social_account_catalog_freshness
    )
    assert (
        legacy_repo.get_social_account_catalog_gap_analysis_status
        is account_catalog_freshness.get_social_account_catalog_gap_analysis_status
    )
    assert (
        catalog_launch.start_social_account_catalog_backfill
        is account_catalog_launch.start_social_account_catalog_backfill
    )
    assert (
        catalog_progress.get_social_account_catalog_run_progress
        is account_catalog_progress.get_social_account_catalog_run_progress
    )


def test_account_catalog_freshness_import_does_not_load_legacy_backed_siblings() -> None:
    backend_root = Path(__file__).resolve().parents[2]
    package_name = "trr_backend.socials.pipelines.account_catalog"
    code = "\n".join(
        [
            "import importlib",
            "import os",
            "import sys",
            f"package = importlib.import_module('{package_name}')",
            f"assert '{package_name}.freshness' not in sys.modules",
            f"assert '{package_name}.launch' not in sys.modules",
            f"assert '{package_name}.progress' not in sys.modules",
            f"assert '{package_name}.review_queue' not in sys.modules",
            "freshness = package.get_social_account_catalog_freshness",
            "assert callable(freshness)",
            f"assert '{package_name}.freshness' in sys.modules",
            f"assert '{package_name}.launch' not in sys.modules",
            f"assert '{package_name}.progress' not in sys.modules",
            f"assert '{package_name}.review_queue' not in sys.modules",
            "assert 'trr_backend.socials.social_season_analytics_impl' not in sys.modules",
            "assert 'trr_backend.repositories.social_season_analytics' not in sys.modules",
            "assert 'DATABASE_URL' not in os.environ",
            "assert 'TRR_SUPABASE_ACCESS_TOKEN' not in os.environ",
            "try:",
            "    freshness('tiktok', 'bravotv')",
            "except ValueError as error:",
            "    assert 'only supported for Instagram' in str(error)",
            "else:",
            "    raise AssertionError('freshness invocation should validate unsupported platforms')",
            "assert 'trr_backend.socials.social_season_analytics_impl' not in sys.modules",
            f"freshness_module = importlib.import_module('{package_name}.freshness')",
            "freshness_module._default_assert_profile_exists = lambda *_args, **_kwargs: None",
            "admin_operations = importlib.import_module('trr_backend.repositories.admin_operations')",
            "admin_operations.get_latest_operation_for_request_payload = lambda **_kwargs: None",
            "gap_status = package.get_social_account_catalog_gap_analysis_status('tiktok', 'bravotv')",
            "assert gap_status['status'] == 'idle'",
            "assert gap_status['platform'] == 'tiktok'",
            "assert gap_status['account_handle'] == 'bravotv'",
            "assert 'trr_backend.socials.social_season_analytics_impl' not in sys.modules",
            "assert 'trr_backend.repositories.social_season_analytics' not in sys.modules",
        ]
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=backend_root,
        capture_output=True,
        env={"PATH": os.environ.get("PATH", ""), "PYTHONDONTWRITEBYTECODE": "1"},
        text=True,
        timeout=120,
        check=False,
    )

    assert result.returncode == 0, result.stderr


def test_standalone_freshness_profile_defaults_match_network_accounts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(account_catalog_freshness, "_optional_exists", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(
        account_catalog_freshness,
        "_default_shared_catalog_total_posts",
        lambda *_args, **_kwargs: 0,
    )

    for platform, account_handle in (
        ("facebook", "bravo"),
        ("instagram", "bravotv"),
        ("instagram", "bravodailydish"),
        ("instagram", "bravowwhl"),
        ("threads", "bravotv"),
        ("threads", "bravodailydish"),
        ("threads", "bravowwhl"),
        ("tiktok", "bravotv"),
        ("tiktok", "bravowwhl"),
        ("twitter", "bravotv"),
        ("twitter", "bravowwhl"),
        ("youtube", "bravo"),
    ):
        account_catalog_freshness._default_assert_profile_exists(platform, account_handle)

    with pytest.raises(LookupError, match="Social account profile not found"):
        account_catalog_freshness._default_assert_profile_exists("instagram", "unknownaccount")


def test_standalone_freshness_profile_lookup_includes_raw_data_owners(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_sql: list[str] = []

    def fake_exists(sql: str, _params: list[object]) -> bool:
        captured_sql.append(sql)
        return "from social.instagram_posts" in sql and "'raw_data'" in sql

    monkeypatch.setattr(account_catalog_freshness, "_optional_exists", fake_exists)
    monkeypatch.setattr(
        account_catalog_freshness,
        "_default_shared_catalog_total_posts",
        lambda *_args, **_kwargs: 0,
    )

    account_catalog_freshness._default_assert_profile_exists("instagram", "rawdataowner")

    materialized_sql = next(sql for sql in captured_sql if "from social.instagram_posts" in sql)
    assert "to_jsonb(p)->'raw_data'->>'source_account'" in materialized_sql
    assert "to_jsonb(p)->'raw_data'->>'owner_username'" in materialized_sql
    assert "to_jsonb(p)->'raw_data'->>'username'" in materialized_sql


def test_standalone_freshness_oldest_date_degrades_on_database_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unavailable(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(account_catalog_freshness, "_fetch_one", unavailable)
    monkeypatch.setattr(
        account_catalog_freshness.pg,
        "is_database_service_unavailable_error",
        lambda _error: True,
    )

    assert account_catalog_freshness._default_catalog_oldest_stored_post_at("instagram", "bravotv") is None
    with pytest.raises(RuntimeError, match="database unavailable"):
        account_catalog_freshness._default_catalog_newest_stored_post_at("instagram", "bravotv")


def test_standalone_freshness_stored_dates_match_instagram_raw_data_owner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fetch_one(sql: str, params: list[object], **_kwargs: object) -> dict[str, object]:
        captured["sql"] = sql
        captured["params"] = params
        return {"stored_at": None}

    monkeypatch.setattr(account_catalog_freshness, "_fetch_one", fetch_one)

    assert account_catalog_freshness._default_catalog_newest_stored_post_at("instagram", "bravotv") is None
    assert "to_jsonb(p) -> 'raw_data' ->> 'source_account'" in str(captured["sql"])
    assert captured["params"] == ["bravotv"]


def test_standalone_freshness_frontier_masks_relation_probe_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    def probe_failure(sql: str, *_args: object, **_kwargs: object) -> None:
        calls.append(sql)
        raise RuntimeError("relation probe unavailable")

    monkeypatch.setattr(account_catalog_freshness, "_fetch_one", probe_failure)

    assert account_catalog_freshness._default_latest_account_frontier("instagram", "bravotv") == {}
    assert len(calls) == 1
    assert "to_regclass" in calls[0]


def test_standalone_live_total_probe_deadline_is_configurable_and_bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SOCIAL_LIVE_PROFILE_TOTAL_PROBE_DEADLINE_SEC", "1")
    assert account_catalog_live_total._live_total_probe_deadline_seconds() == 4.0
    monkeypatch.setenv("SOCIAL_LIVE_PROFILE_TOTAL_PROBE_DEADLINE_SEC", "9.5")
    assert account_catalog_live_total._live_total_probe_deadline_seconds() == 9.5
    monkeypatch.setenv("SOCIAL_LIVE_PROFILE_TOTAL_PROBE_DEADLINE_SEC", "invalid")
    assert account_catalog_live_total._live_total_probe_deadline_seconds() == 22.0


def test_standalone_live_total_budget_starts_before_auth_loading(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    ticks = iter((100.0, 101.0, 102.0))

    def monotonic() -> float:
        events.append("clock")
        return next(ticks)

    def load_auth() -> dict[str, str]:
        events.append("auth")
        return {}

    class FakeScraper:
        def __init__(self, **_kwargs: object) -> None:
            pass

        def fetch_profile_info(self, *_args: object, **_kwargs: object) -> dict[str, int]:
            return {"total_posts": 123}

        def _extract_profile_total_posts(self, payload: dict[str, int], **_kwargs: object) -> int:
            return payload["total_posts"]

    account_catalog_live_total._LIVE_TOTAL_CACHE.clear()
    monkeypatch.setattr(account_catalog_live_total.time_module, "monotonic", monotonic)
    monkeypatch.setattr(account_catalog_live_total, "_load_instagram_auth_cookies", load_auth)
    monkeypatch.setattr(account_catalog_live_total, "_instagram_scraper_type", lambda: FakeScraper)

    assert account_catalog_live_total.cached_instagram_live_profile_total_posts("bravotv") == 123
    assert events[:3] == ["clock", "clock", "auth"]


def test_standalone_live_total_auth_resolver_flag_preserves_legacy_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.socials.instagram import auth_resolver

    monkeypatch.delenv("INSTAGRAM_AUTH_RESOLVER_V2", raising=False)
    monkeypatch.setattr(
        account_catalog_live_total,
        "_load_legacy_instagram_cookies",
        lambda: {"sessionid": "legacy"},
    )
    assert account_catalog_live_total._load_instagram_auth_cookies() == {"sessionid": "legacy"}

    captured: dict[str, object] = {}

    def resolve(**kwargs: object) -> object:
        captured.update(kwargs)
        return type("AuthSession", (), {"cookies": {"sessionid": "resolver"}})()

    monkeypatch.setenv("INSTAGRAM_AUTH_RESOLVER_V2", "true")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COOKIE_VALIDATION_USERNAME", "ValidationAccount")
    monkeypatch.setattr(auth_resolver, "resolve_instagram_auth_session", resolve)

    assert account_catalog_live_total._load_instagram_auth_cookies() == {"sessionid": "resolver"}
    assert captured == {
        "browser_account_id": "validationaccount",
        "caller_context": "legacy_loader",
        "require_validation": True,
    }


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
    assert profile_reads.get_social_account_profile_summary is account_profile_common.get_social_account_profile_summary
    assert profile_reads.get_social_account_profile_posts is account_profile_common.get_social_account_profile_posts
    assert (
        profile_reads.get_social_account_profile_comments is account_profile_common.get_social_account_profile_comments
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


def _run_account_profile_contract_script(source: str) -> None:
    result = subprocess.run(
        [sys.executable, "-B", "-c", dedent(source)],
        cwd=Path(__file__).resolve().parents[2],
        capture_output=True,
        env={"PATH": os.environ.get("PATH", ""), "PYTHONDONTWRITEBYTECODE": "1"},
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, (
        f"fresh account-profile contract process failed\n"
        f"stdout:\n{result.stdout or '<empty>'}\n"
        f"stderr:\n{result.stderr or '<empty>'}"
    )


def test_account_profile_nine_normal_import_leaders_reach_frozen_states() -> None:
    expected = {
        "trr_backend.socials.read_models.account_profile.common": "UNCONFIGURED",
        "trr_backend.socials.read_models.account_profile": "UNCONFIGURED",
        "trr_backend.socials.read_models.account_profile.instagram": "UNCONFIGURED",
        "trr_backend.socials.account_catalog.profile_reads": "UNCONFIGURED",
        "trr_backend.socials.account_catalog": "UNCONFIGURED",
        "trr_backend.socials.profile_dashboard": "UNCONFIGURED",
        "trr_backend.socials.social_season_analytics_impl": "READY",
        "trr_backend.repositories.social_season_analytics": "READY",
        "trr_backend.socials.control_plane.shared_accounts": "READY",
    }
    for leader, expected_state in expected.items():
        _run_account_profile_contract_script(
            f"""
            import importlib
            import sys

            common_name = "trr_backend.socials.read_models.account_profile.common"
            provider_name = "trr_backend.socials.social_season_analytics_impl"
            leader = {leader!r}
            imported = importlib.import_module(leader)
            common = sys.modules[common_name]
            assert common._PROVIDER_STATE == {expected_state!r}
            if {expected_state!r} == "UNCONFIGURED":
                assert provider_name not in sys.modules
                assert common._PROVIDER_NAMESPACE is None
            else:
                provider = sys.modules[provider_name]
                assert common._PROVIDER_NAMESPACE is provider.__dict__
                if leader in {{provider_name, "trr_backend.repositories.social_season_analytics"}}:
                    assert imported is provider
            """
        )


def test_account_catalog_package_lazy_exports_preserve_public_contract() -> None:
    _run_account_profile_contract_script(
        """
        import importlib
        import sys

        common_name = "trr_backend.socials.read_models.account_profile.common"
        package_name = "trr_backend.socials.account_catalog"
        provider_name = "trr_backend.socials.social_season_analytics_impl"
        launch_name = "trr_backend.socials.pipelines.account_catalog.launch"
        progress_name = "trr_backend.socials.pipelines.account_catalog.progress"
        package = importlib.import_module(package_name)
        common = sys.modules[common_name]
        eager = (
            "get_social_account_profile_collaborators_tags",
            "get_social_account_profile_comments",
            "get_social_account_profile_hashtags",
            "get_social_account_profile_posts",
            "get_social_account_profile_summary",
        )
        lazy = {
            "begin_social_account_catalog_backfill_launch": launch_name,
            "finalize_social_account_catalog_backfill_launch": launch_name,
            "get_social_account_catalog_run_progress": progress_name,
            "get_instagram_catalog_launch_capacity": launch_name,
            "launch_social_account_catalog_backfill": launch_name,
            "start_social_account_catalog_backfill": launch_name,
        }
        assert package.__all__ == [
            "begin_social_account_catalog_backfill_launch",
            "finalize_social_account_catalog_backfill_launch",
            "get_social_account_catalog_run_progress",
            "get_instagram_catalog_launch_capacity",
            *eager,
            "launch_social_account_catalog_backfill",
            "start_social_account_catalog_backfill",
        ]
        assert common._PROVIDER_STATE == "UNCONFIGURED"
        assert provider_name not in sys.modules
        assert launch_name not in sys.modules
        assert progress_name not in sys.modules
        assert all(getattr(package, name) is getattr(common, name) for name in eager)
        assert all(name not in package.__dict__ for name in lazy)
        assert set(package.__all__).issubset(dir(package))
        try:
            getattr(package, "not_a_real_export")
        except AttributeError as error:
            assert str(error) == (
                "module 'trr_backend.socials.account_catalog' has no attribute "
                "'not_a_real_export'"
            )
        else:
            raise AssertionError("unknown account-catalog export did not fail")

        provider = importlib.import_module(provider_name)
        assert common._PROVIDER_STATE == "READY"
        assert common._PROVIDER_NAMESPACE is provider.__dict__
        for name, module_name in lazy.items():
            canonical = importlib.import_module(module_name)
            first = getattr(package, name)
            assert first is getattr(canonical, name)
            assert package.__dict__[name] is first
            assert getattr(package, name) is first
        """
    )


def test_account_profile_provider_publication_is_atomic_retryable_and_identity_bound() -> None:
    _run_account_profile_contract_script(
        """
        import importlib

        common = importlib.import_module(
            "trr_backend.socials.read_models.account_profile.common"
        )
        assert common._PROVIDER_STATE == "UNCONFIGURED"
        stable_exports = {name: getattr(common, name) for name in common.__all__}
        baseline_post_item = common._CORE_SOCIAL_ACCOUNT_PROFILE_POST_ITEM
        provider = {
            name: (lambda *args, **kwargs: (name, args, kwargs))
            for name in common._LOCAL_ROOM_NAMES
        }
        provider.update(
            {
                "_SOCIAL_ACCOUNT_PROFILE_DEFAULT_PAGE_SIZE": 25,
                "_normalize_social_account_profile_platform": lambda value: str(value).strip().lower(),
                "ordinary_a": object(),
                "ordinary_b": object(),
            }
        )
        fallback_name = "_fetch_materialized_comments_only_profile_rows_page"
        fallback_calls = []

        def configured_fallback(platform, account_handle, **kwargs):
            fallback_calls.append(("configured", platform, account_handle, kwargs))
            return ([{"source": "configured"}], 7)

        provider[fallback_name] = configured_fallback
        original_publish = common._publish_provider_binding
        published = []

        class PublicationFailure(BaseException):
            pass

        def fail_second(name, value):
            published.append(name)
            original_publish(name, value)
            if len(published) == 2:
                raise PublicationFailure("deterministic staged failure")

        common._publish_provider_binding = fail_second
        try:
            common._configure_legacy_provider(provider)
        except PublicationFailure as error:
            assert str(error) == "deterministic staged failure"
        else:
            raise AssertionError("partial provider publication did not fail")
        finally:
            common._publish_provider_binding = original_publish

        assert common._PROVIDER_STATE == "UNCONFIGURED"
        assert common._PROVIDER_NAMESPACE is None
        assert common._IMPORTED_CORE_NAMES == set()
        assert common._CORE_ROOM_WRAPPERS == {}
        assert common._CORE_SOCIAL_ACCOUNT_PROFILE_POST_ITEM is baseline_post_item
        assert common._SOCIAL_ACCOUNT_PROFILE_DEFAULT_PAGE_SIZE == 25
        assert "_normalize_social_account_profile_platform" not in common.__dict__
        assert "ordinary_a" not in common.__dict__
        assert "ordinary_b" not in common.__dict__
        assert {name: getattr(common, name) for name in common.__all__} == stable_exports

        common._configure_legacy_provider(provider)
        assert common._PROVIDER_STATE == "READY"
        assert common._PROVIDER_NAMESPACE is provider
        assert common._CORE_SOCIAL_ACCOUNT_PROFILE_POST_ITEM is provider[
            "_social_account_profile_post_item"
        ]
        assert common.ordinary_a is provider["ordinary_a"]
        assert common.ordinary_b is provider["ordinary_b"]
        assert {name: getattr(common, name) for name in common.__all__} == stable_exports
        local_fallback = common._LOCAL_ROOM_FUNCTIONS[fallback_name]
        original_room_baseline = common._CORE_ROOM_WRAPPERS[fallback_name]
        sentinel_conn = object()
        assert original_room_baseline is configured_fallback
        assert local_fallback(
            "twitter",
            "BravoTV",
            page=2,
            page_size=5,
            search="needle",
            comment_filter="missing",
            sort_by="created",
            sort_dir="desc",
            conn=sentinel_conn,
        ) == ([{"source": "configured"}], 7)
        assert fallback_calls == [
            (
                "configured",
                "twitter",
                "BravoTV",
                {
                    "page": 2,
                    "page_size": 5,
                    "search": "needle",
                    "comment_filter": "missing",
                    "sort_by": "created",
                    "sort_dir": "desc",
                    "conn": sentinel_conn,
                },
            )
        ]

        before = (
            common._PROVIDER_STATE,
            common._PROVIDER_NAMESPACE,
            set(common._IMPORTED_CORE_NAMES),
            dict(common._CORE_ROOM_WRAPPERS),
            common.ordinary_a,
            common.ordinary_b,
        )
        common._configure_legacy_provider(provider)
        assert before == (
            common._PROVIDER_STATE,
            common._PROVIDER_NAMESPACE,
            set(common._IMPORTED_CORE_NAMES),
            dict(common._CORE_ROOM_WRAPPERS),
            common.ordinary_a,
            common.ordinary_b,
        )
        patched_calls = []

        def patched_fallback(platform, account_handle, **kwargs):
            patched_calls.append((platform, account_handle, kwargs))
            return ([{"source": "patched"}], 11)

        provider[fallback_name] = patched_fallback
        assert local_fallback(
            "twitter",
            "bravotv",
            page=1,
            page_size=3,
        ) == ([{"source": "patched"}], 11)
        assert len(patched_calls) == 1
        provider[fallback_name] = configured_fallback
        assert local_fallback(
            "twitter",
            "bravotv",
            page=1,
            page_size=3,
        ) == ([{"source": "configured"}], 7)
        assert common._CORE_ROOM_WRAPPERS[fallback_name] is original_room_baseline
        different = dict(provider)
        try:
            common._configure_legacy_provider(different)
        except RuntimeError as error:
            assert str(error) == (
                "ACCOUNT_PROFILE_PROVIDER_MISMATCH: account-profile provider is "
                "already configured with a different mapping"
            )
        else:
            raise AssertionError("different provider identity did not fail")
        assert common._PROVIDER_NAMESPACE is provider
        assert common.ordinary_a is provider["ordinary_a"]
        assert common.ordinary_b is provider["ordinary_b"]
        """
    )


def test_account_profile_patch_families_and_room_callable_restore_exactly() -> None:
    _run_account_profile_contract_script(
        """
        import importlib

        provider = importlib.import_module(
            "trr_backend.socials.social_season_analytics_impl"
        )
        repository = importlib.import_module(
            "trr_backend.repositories.social_season_analytics"
        )
        common = importlib.import_module(
            "trr_backend.socials.read_models.account_profile.common"
        )
        assert repository is provider
        assert common._PROVIDER_NAMESPACE is provider.__dict__
        room = "get_social_account_profile_summary"
        original_provider_room = provider.__dict__[room]
        original_local_room = common._LOCAL_ROOM_FUNCTIONS[room]
        assert common._CORE_ROOM_WRAPPERS[room] is original_provider_room
        assert common._room_callable(room, original_local_room) is original_local_room

        def provider_replacement(*args, **kwargs):
            return args, kwargs

        provider.__dict__[room] = provider_replacement
        assert common._room_callable(room, original_local_room) is provider_replacement
        provider.__dict__[room] = original_provider_room
        assert common._room_callable(room, original_local_room) is original_local_room

        def local_replacement(*args, **kwargs):
            return args, kwargs

        assert common._room_callable(room, local_replacement) is local_replacement

        copied = "_catalog_recent_runs_header"
        original_copied = provider.__dict__[copied]
        copied_replacement = lambda *args, **kwargs: []
        provider.__dict__[copied] = copied_replacement
        common._sync_core_overrides()
        assert common.__dict__[copied] is copied_replacement
        provider.__dict__[copied] = original_copied
        common._sync_core_overrides()
        assert common.__dict__[copied] is original_copied

        post_item = "_social_account_profile_post_item"
        original_post_item = provider.__dict__[post_item]
        assert common._CORE_SOCIAL_ACCOUNT_PROFILE_POST_ITEM is original_post_item
        provider.__dict__[post_item] = provider_replacement
        assert (
            common._room_callable(post_item, common._LOCAL_ROOM_FUNCTIONS[post_item])
            is provider_replacement
        )
        provider.__dict__[post_item] = original_post_item
        assert (
            common._room_callable(post_item, common._LOCAL_ROOM_FUNCTIONS[post_item])
            is common._LOCAL_ROOM_FUNCTIONS[post_item]
        )
        assert common._CORE_SOCIAL_ACCOUNT_PROFILE_POST_ITEM is original_post_item

        ordinary_cases = {
            "_instagram_payload_sidecar_sql": (
                (),
                {"row_kind": "post", "row_alias": "p", "mode": "dual_read"},
                ("patched-sidecar", "patched-projection"),
            ),
            "_instagram_payload_rows_for_read": (
                ([{"id": "row-1"}],),
                {"row_kind": "post", "mode": "dual_read", "surface": "probe"},
                [{"id": "patched-row"}],
            ),
            "_log_instagram_payload_schema_unavailable": (
                (),
                {"surface": "probe", "entity_identity": "bravotv"},
                None,
            ),
        }
        for binding, (args, kwargs, expected) in ordinary_cases.items():
            original = provider.__dict__[binding]
            calls = []

            def replacement(*call_args, _expected=expected, **call_kwargs):
                calls.append((call_args, call_kwargs))
                return _expected

            provider.__dict__[binding] = replacement
            common._sync_core_overrides()
            assert common.__dict__[binding] is replacement
            assert common.__dict__[binding](*args, **kwargs) == expected
            assert calls == [(args, kwargs)]
            provider.__dict__[binding] = original
            common._sync_core_overrides()
            assert common.__dict__[binding] is original

        fallback = "_fetch_materialized_comments_only_profile_rows_page"
        original_provider_fallback = provider.__dict__[fallback]
        original_local_fallback = common._LOCAL_ROOM_FUNCTIONS[fallback]
        assert common._CORE_ROOM_WRAPPERS[fallback] is original_provider_fallback
        fallback_calls = []

        def repository_fallback(platform, account_handle, **kwargs):
            fallback_calls.append((platform, account_handle, kwargs))
            return ([{"source": "repository-patch"}], 13)

        repository.__dict__[fallback] = repository_fallback
        assert original_local_fallback(
            "twitter",
            "bravotv",
            page=1,
            page_size=4,
        ) == ([{"source": "repository-patch"}], 13)
        assert len(fallback_calls) == 1
        repository.__dict__[fallback] = original_provider_fallback
        assert common._require_provider_ready()[fallback] is original_provider_fallback
        try:
            original_local_fallback(
                "threads",
                "bravotv",
                page=1,
                page_size=4,
            )
        except ValueError as error:
            assert str(error) == "Unsupported comments-only fast path platform: threads"
        else:
            raise AssertionError("restored provider fallback did not run")
        assert common._CORE_ROOM_WRAPPERS[fallback] is original_provider_fallback
        """
    )
