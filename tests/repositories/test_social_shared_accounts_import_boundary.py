from __future__ import annotations

import ast
import importlib
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

import trr_backend.repositories.social_season_analytics as legacy_repository
import trr_backend.socials.control_plane.dispatch_runtime as dispatch_runtime
import trr_backend.socials.control_plane.shared_accounts as shared_accounts
import trr_backend.socials.social_season_analytics_impl as legacy_impl

BACKEND_ROOT = Path(__file__).resolve().parents[2]
SHARED_ACCOUNTS_PATH = BACKEND_ROOT / "trr_backend" / "socials" / "control_plane" / "shared_accounts.py"
PACKAGE_MODULE = "trr_backend.socials.control_plane"
SHARED_ACCOUNTS_MODULE = f"{PACKAGE_MODULE}.shared_accounts"
DISPATCH_RUNTIME_MODULE = f"{PACKAGE_MODULE}.dispatch_runtime"
LEGACY_IMPL_MODULE = "trr_backend.socials.social_season_analytics_impl"
LEGACY_REPOSITORY_MODULE = "trr_backend.repositories.social_season_analytics"

COPIED_BINDINGS = (
    ("_default_targets", "_default_targets"),
    ("_normalize_catalog_backfill_window", "_normalize_catalog_backfill_window"),
    (
        "_shared_account_catalog_requires_modal_executor",
        "_shared_account_catalog_requires_modal_executor",
    ),
    ("cancel_shared_run", "cancel_shared_run"),
    ("dismiss_social_account_catalog_run", "dismiss_social_account_catalog_run"),
    ("get_season_context", "get_season_context"),
    ("get_social_account_catalog_freshness", "get_social_account_catalog_freshness"),
    (
        "get_social_account_catalog_gap_analysis_status",
        "get_social_account_catalog_gap_analysis_status",
    ),
    ("get_social_account_catalog_posts", "get_social_account_catalog_posts"),
    (
        "get_social_account_catalog_review_queue",
        "get_social_account_catalog_review_queue",
    ),
    (
        "get_social_account_catalog_verification",
        "get_social_account_catalog_verification",
    ),
    (
        "get_social_account_profile_hashtag_timeline",
        "get_social_account_profile_hashtag_timeline",
    ),
    ("get_targets", "get_targets"),
    ("list_shared_review_queue", "list_shared_review_queue"),
    ("put_social_account_profile_hashtags", "put_social_account_profile_hashtags"),
    ("put_targets", "put_targets"),
    ("resolve_shared_review_queue_item", "resolve_shared_review_queue_item"),
    (
        "resolve_social_account_catalog_review_queue_item",
        "resolve_social_account_catalog_review_queue_item",
    ),
    (
        "_legacy_cancel_social_account_catalog_run",
        "cancel_social_account_catalog_run",
    ),
)
EXPECTED_ALL = (
    "_batch_upsert_shared_catalog_instagram_posts",
    "_default_targets",
    "_normalize_catalog_backfill_window",
    "_shared_account_catalog_requires_modal_executor",
    "batch_upsert_shared_catalog_instagram_posts",
    "cancel_shared_run",
    "cancel_social_account_catalog_run",
    "default_targets",
    "dismiss_social_account_catalog_run",
    "get_season_context",
    "get_season_shared_status",
    "get_shared_account_sources",
    "get_social_account_catalog_freshness",
    "get_social_account_catalog_gap_analysis_status",
    "get_social_account_catalog_posts",
    "get_social_account_catalog_review_queue",
    "get_social_account_catalog_run_progress",
    "get_social_account_catalog_verification",
    "get_social_account_profile_collaborators_tags",
    "get_social_account_profile_comments",
    "get_social_account_profile_hashtag_timeline",
    "get_social_account_profile_hashtags",
    "get_social_account_profile_posts",
    "get_social_account_profile_summary",
    "get_targets",
    "list_shared_review_queue",
    "list_shared_runs",
    "normalize_catalog_backfill_window",
    "put_shared_account_sources",
    "put_social_account_profile_hashtags",
    "put_targets",
    "resolve_shared_review_queue_item",
    "resolve_social_account_catalog_review_queue_item",
    "shared_account_catalog_requires_modal_executor",
)
IMPORT_ORDERS = (
    pytest.param(
        (
            PACKAGE_MODULE,
            SHARED_ACCOUNTS_MODULE,
            DISPATCH_RUNTIME_MODULE,
            LEGACY_IMPL_MODULE,
            LEGACY_REPOSITORY_MODULE,
        ),
        id="package-root-first",
    ),
    pytest.param(
        (
            SHARED_ACCOUNTS_MODULE,
            PACKAGE_MODULE,
            DISPATCH_RUNTIME_MODULE,
            LEGACY_IMPL_MODULE,
            LEGACY_REPOSITORY_MODULE,
        ),
        id="exact-leaf-first",
    ),
    pytest.param(
        (
            DISPATCH_RUNTIME_MODULE,
            SHARED_ACCOUNTS_MODULE,
            PACKAGE_MODULE,
            LEGACY_IMPL_MODULE,
            LEGACY_REPOSITORY_MODULE,
        ),
        id="dispatch-runtime-first",
    ),
    pytest.param(
        (
            LEGACY_IMPL_MODULE,
            SHARED_ACCOUNTS_MODULE,
            PACKAGE_MODULE,
            DISPATCH_RUNTIME_MODULE,
            LEGACY_REPOSITORY_MODULE,
        ),
        id="monolith-first",
    ),
    pytest.param(
        (
            LEGACY_REPOSITORY_MODULE,
            SHARED_ACCOUNTS_MODULE,
            PACKAGE_MODULE,
            DISPATCH_RUNTIME_MODULE,
            LEGACY_IMPL_MODULE,
        ),
        id="repository-alias-first",
    ),
)


def _run_fresh_process(import_order: tuple[str, ...]) -> None:
    code = "\n".join(
        (
            "import importlib",
            "import sys",
            f"for module_name in {import_order!r}:",
            "    importlib.import_module(module_name)",
            f"package = importlib.import_module({PACKAGE_MODULE!r})",
            f"shared = importlib.import_module({SHARED_ACCOUNTS_MODULE!r})",
            f"dispatch = importlib.import_module({DISPATCH_RUNTIME_MODULE!r})",
            f"legacy = importlib.import_module({LEGACY_IMPL_MODULE!r})",
            f"repository = importlib.import_module({LEGACY_REPOSITORY_MODULE!r})",
            "assert dispatch.legacy is legacy",
            "assert repository is legacy",
            "assert repository.__dict__ is legacy.__dict__",
            f"assert sys.modules[{LEGACY_REPOSITORY_MODULE!r}] is legacy",
            "assert dispatch.legacy.__dict__ is repository.__dict__",
            f"for local_name, legacy_name in {COPIED_BINDINGS!r}:",
            "    assert getattr(shared, local_name) is getattr(legacy, legacy_name)",
            "assert package._default_targets is shared._default_targets",
            "assert not hasattr(shared, '_legacy')",
        )
    )
    environment = dict(os.environ)
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    result = subprocess.run(
        [sys.executable, "-B", "-c", code],
        cwd=BACKEND_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, (
        f"fresh child process failed\nstdout:\n{result.stdout or '<empty>'}\nstderr:\n{result.stderr or '<empty>'}"
    )


@pytest.mark.parametrize("import_order", IMPORT_ORDERS)
def test_fresh_process_import_orders_preserve_exact_legacy_identity(
    import_order: tuple[str, ...],
) -> None:
    _run_fresh_process(import_order)


def test_dispatch_runtime_monolith_and_repository_share_exact_module_identity() -> None:
    assert dispatch_runtime.legacy is legacy_impl
    assert legacy_repository is legacy_impl
    assert legacy_repository.__dict__ is legacy_impl.__dict__
    assert dispatch_runtime.legacy.__dict__ is legacy_repository.__dict__
    assert sys.modules[LEGACY_REPOSITORY_MODULE] is legacy_impl


def test_remaining_legacy_bindings_are_import_time_identity_copies() -> None:
    for local_name, legacy_name in COPIED_BINDINGS:
        assert getattr(shared_accounts, local_name) is getattr(legacy_impl, legacy_name)


def test_patch_before_reload_is_copied(monkeypatch: pytest.MonkeyPatch) -> None:
    replacement = object()
    try:
        with monkeypatch.context() as scoped:
            scoped.setattr(legacy_impl, "get_targets", replacement)
            reloaded = importlib.reload(shared_accounts)
            assert reloaded.get_targets is replacement
    finally:
        importlib.reload(shared_accounts)

    assert shared_accounts.get_targets is legacy_impl.get_targets


def test_patch_after_import_does_not_rebind_copied_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    copied = shared_accounts.get_targets
    replacement = object()

    monkeypatch.setattr(legacy_impl, "get_targets", replacement)

    assert shared_accounts.get_targets is copied
    assert shared_accounts.get_targets is not replacement


def test_public_aliases_preserve_all_three_private_binding_identities() -> None:
    assert shared_accounts.default_targets is shared_accounts._default_targets
    assert shared_accounts.normalize_catalog_backfill_window is shared_accounts._normalize_catalog_backfill_window
    assert (
        shared_accounts.shared_account_catalog_requires_modal_executor
        is shared_accounts._shared_account_catalog_requires_modal_executor
    )


def test_shared_source_operations_use_canonical_leaf() -> None:
    from trr_backend.socials.control_plane import shared_source_config

    assert shared_accounts.get_shared_account_sources is shared_source_config.get_shared_account_sources
    assert shared_accounts.put_shared_account_sources is shared_source_config.put_shared_account_sources
    assert shared_accounts.get_shared_account_sources is not legacy_impl.get_shared_account_sources
    assert shared_accounts.put_shared_account_sources is not legacy_impl.put_shared_account_sources


def test_all_exports_remain_exact_and_ordered() -> None:
    assert tuple(shared_accounts.__all__) == EXPECTED_ALL


def test_cancellation_fallback_forwards_exact_arguments_result_and_conn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = object()
    marker = object()
    calls: list[dict[str, Any]] = []

    def fallback(**kwargs: Any) -> object:
        calls.append(kwargs)
        return marker

    monkeypatch.setattr(shared_accounts.pg, "fetch_one", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        shared_accounts,
        "_legacy_cancel_social_account_catalog_run",
        fallback,
    )

    result = shared_accounts.cancel_social_account_catalog_run(
        platform="Instagram",
        account_handle="@Housewife",
        run_id="run-1",
        cancelled_by="operator",
        reconcile_summary=False,
        conn=connection,
    )

    assert callable(shared_accounts._legacy_cancel_social_account_catalog_run)
    assert result is marker
    assert calls == [
        {
            "platform": "Instagram",
            "account_handle": "@Housewife",
            "run_id": "run-1",
            "cancelled_by": "operator",
            "reconcile_summary": False,
            "conn": connection,
        }
    ]


def test_cancellation_fallback_preserves_exception_and_conn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = object()
    error = RuntimeError("legacy cancellation failed")
    observed_connections: list[object] = []

    def fetch_one(*_args: Any, **kwargs: Any) -> None:
        observed_connections.append(kwargs["conn"])

    def fallback(**kwargs: Any) -> None:
        observed_connections.append(kwargs["conn"])
        raise error

    monkeypatch.setattr(shared_accounts.pg, "fetch_one", fetch_one)
    monkeypatch.setattr(
        shared_accounts,
        "_legacy_cancel_social_account_catalog_run",
        fallback,
    )

    with pytest.raises(RuntimeError) as raised:
        shared_accounts.cancel_social_account_catalog_run(
            platform="instagram",
            account_handle="housewife",
            run_id="run-2",
            conn=connection,
        )

    assert raised.value is error
    assert observed_connections == [connection, connection]


def test_source_uses_dispatch_proxy_without_direct_legacy_import_or_dynamic_lookup() -> None:
    tree = ast.parse(SHARED_ACCOUNTS_PATH.read_text())
    forbidden_modules = {
        LEGACY_IMPL_MODULE,
        LEGACY_REPOSITORY_MODULE,
    }
    direct_legacy_imports = [
        node
        for node in ast.walk(tree)
        if (isinstance(node, ast.ImportFrom) and node.module in forbidden_modules)
        or (isinstance(node, ast.Import) and any(alias.name in forbidden_modules for alias in node.names))
    ]
    dispatch_proxy_imports = [
        node
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == DISPATCH_RUNTIME_MODULE
        and [(alias.name, alias.asname) for alias in node.names] == [("legacy", "_legacy")]
    ]
    dynamic_legacy_lookups = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "getattr"
        and node.args
        and isinstance(node.args[0], ast.Name)
        and node.args[0].id == "_legacy"
    ]

    assert direct_legacy_imports == []
    assert len(dispatch_proxy_imports) == 1
    assert dynamic_legacy_lookups == []
    assert "_legacy" not in shared_accounts.__dict__


def test_shared_accounts_normal_import_preserves_ready_identity_boundary() -> None:
    code = "\n".join(
        (
            "import importlib",
            "import sys",
            f"shared = importlib.import_module({SHARED_ACCOUNTS_MODULE!r})",
            f"provider = importlib.import_module({LEGACY_IMPL_MODULE!r})",
            f"repository = importlib.import_module({LEGACY_REPOSITORY_MODULE!r})",
            "common = importlib.import_module(",
            "    'trr_backend.socials.read_models.account_profile.common'",
            ")",
            "assert common._PROVIDER_STATE == 'READY'",
            "assert common._PROVIDER_NAMESPACE is provider.__dict__",
            "assert repository is provider",
            f"assert sys.modules[{LEGACY_REPOSITORY_MODULE!r}] is provider",
            f"for local_name, provider_name in {COPIED_BINDINGS!r}:",
            "    assert getattr(shared, local_name) is getattr(provider, provider_name)",
        )
    )
    environment = dict(os.environ)
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    result = subprocess.run(
        [sys.executable, "-B", "-c", code],
        cwd=BACKEND_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
