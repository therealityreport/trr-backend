"""Compatibility contract for the social profile-reads import boundary."""

from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path
from textwrap import dedent

BACKEND_ROOT = Path(__file__).resolve().parents[3]
PROFILE_READS_PATH = BACKEND_ROOT / "trr_backend" / "socials" / "api" / "handlers" / "profile_reads.py"
DISPATCH_RUNTIME_MODULE = "trr_backend.socials.control_plane.dispatch_runtime"
IMPLEMENTATION_MODULE = "trr_backend.socials.social_season_analytics_impl"
REPOSITORY_ALIAS_MODULE = "trr_backend.repositories.social_season_analytics"


def _run_fresh_python(script: str | list[str]) -> None:
    source = dedent(script) if isinstance(script, str) else "\n".join(script)
    result = subprocess.run(
        [sys.executable, "-c", source],
        cwd=BACKEND_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_profile_reads_uses_only_function_scoped_dispatch_runtime_proxy_imports() -> None:
    source = PROFILE_READS_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    proxy_imports: list[ast.ImportFrom] = []
    direct_legacy_imports: list[ast.Import | ast.ImportFrom] = []

    def visit(node: ast.AST, *, function_depth: int = 0) -> None:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            function_depth += 1
        if isinstance(node, ast.Import):
            if any(alias.name in {IMPLEMENTATION_MODULE, REPOSITORY_ALIAS_MODULE} for alias in node.names):
                direct_legacy_imports.append(node)
        elif isinstance(node, ast.ImportFrom):
            if node.module in {IMPLEMENTATION_MODULE, REPOSITORY_ALIAS_MODULE}:
                direct_legacy_imports.append(node)
            if node.module == DISPATCH_RUNTIME_MODULE:
                assert function_depth > 0
                assert [(alias.name, alias.asname) for alias in node.names] == [("legacy", "social_core")]
                proxy_imports.append(node)
        for child in ast.iter_child_nodes(node):
            visit(child, function_depth=function_depth)

    visit(tree)

    assert not direct_legacy_imports
    assert len(proxy_imports) == 15
    assert IMPLEMENTATION_MODULE not in source
    assert REPOSITORY_ALIAS_MODULE not in source

    functions = {node.name: node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}
    budget_import = functions["get_catalog_budget_decision"].body[0]
    assert isinstance(budget_import, ast.ImportFrom)
    assert budget_import.module == "trr_backend.socials.control_plane.budget"
    assert [(alias.name, alias.asname) for alias in budget_import.names] == [("build_budget_decision", None)]
    diagnostics_import = functions["get_catalog_run_diagnostics"].body[0]
    assert isinstance(diagnostics_import, ast.ImportFrom)
    assert diagnostics_import.module == "trr_backend.socials.pipelines.account_catalog.progress"
    assert [(alias.name, alias.asname) for alias in diagnostics_import.names] == [
        ("get_social_account_catalog_run_diagnostics", None)
    ]


def test_profile_reads_cold_import_and_first_call_preserve_import_order() -> None:
    _run_fresh_python(
        [
            "import importlib",
            "import sys",
            f"profile_name = {'trr_backend.socials.api.handlers.profile_reads'!r}",
            f"runtime_name = {DISPATCH_RUNTIME_MODULE!r}",
            f"implementation_name = {IMPLEMENTATION_MODULE!r}",
            f"repository_name = {REPOSITORY_ALIAS_MODULE!r}",
            "assert profile_name not in sys.modules",
            "assert runtime_name not in sys.modules",
            "assert implementation_name not in sys.modules",
            "assert repository_name not in sys.modules",
            "profile_reads = importlib.import_module(profile_name)",
            "assert runtime_name not in sys.modules",
            "assert implementation_name not in sys.modules",
            "assert repository_name not in sys.modules",
            "assert profile_reads.normalize_profile_summary_detail(None) == 'lite'",
            "runtime = importlib.import_module(runtime_name)",
            "implementation = importlib.import_module(implementation_name)",
            "repository = importlib.import_module(repository_name)",
            "assert runtime.legacy is implementation",
            "assert repository is implementation",
            "assert runtime.legacy.__dict__ is repository.__dict__",
        ]
    )


def test_profile_reads_all_wrappers_preserve_live_proxy_passthrough() -> None:
    _run_fresh_python(
        f"""
        import importlib
        import inspect

        implementation_name = {IMPLEMENTATION_MODULE!r}
        repository_name = {REPOSITORY_ALIAS_MODULE!r}
        runtime_name = {DISPATCH_RUNTIME_MODULE!r}
        repository = importlib.import_module(repository_name)
        early_marker = object()
        repository._normalize_social_account_profile_summary_detail = (
            lambda value: early_marker
        )
        profile_reads = importlib.import_module(
            "trr_backend.socials.api.handlers.profile_reads"
        )
        assert profile_reads.normalize_profile_summary_detail(None) is early_marker
        runtime = importlib.import_module(runtime_name)
        implementation = importlib.import_module(implementation_name)
        assert runtime.legacy is implementation
        assert repository is implementation
        assert runtime.legacy.__dict__ is repository.__dict__

        required = "<required>"
        signature_contracts = {{
            "normalize_profile_summary_detail": (
                (
                    (
                        "value",
                        "POSITIONAL_OR_KEYWORD",
                        "str | None",
                        required,
                    ),
                ),
                "str",
            ),
            "get_profile_summary": (
                (
                    ("platform", "KEYWORD_ONLY", "str", required),
                    ("account_handle", "KEYWORD_ONLY", "str", required),
                    ("detail", "KEYWORD_ONLY", "str", required),
                ),
                "dict[str, Any]",
            ),
            "get_live_profile_total": (
                (
                    ("platform", "KEYWORD_ONLY", "str", required),
                    ("account_handle", "KEYWORD_ONLY", "str", required),
                ),
                "dict[str, Any]",
            ),
            "get_profile_posts": (
                (
                    ("platform", "KEYWORD_ONLY", "str", required),
                    ("account_handle", "KEYWORD_ONLY", "str", required),
                    ("page", "KEYWORD_ONLY", "int", required),
                    ("page_size", "KEYWORD_ONLY", "int", required),
                    ("search", "KEYWORD_ONLY", "str | None", required),
                    ("comments_only", "KEYWORD_ONLY", "bool", required),
                    ("comment_filter", "KEYWORD_ONLY", "str | None", required),
                    ("sort_by", "KEYWORD_ONLY", "str | None", required),
                    ("sort_dir", "KEYWORD_ONLY", "str | None", required),
                ),
                "dict[str, Any]",
            ),
            "get_profile_comments": (
                (
                    ("platform", "KEYWORD_ONLY", "str", required),
                    ("account_handle", "KEYWORD_ONLY", "str", required),
                    ("page", "KEYWORD_ONLY", "int", required),
                    ("page_size", "KEYWORD_ONLY", "int", required),
                    ("post_source_id", "KEYWORD_ONLY", "str | None", required),
                    ("search", "KEYWORD_ONLY", "str | None", required),
                    ("sort_by", "KEYWORD_ONLY", "str | None", required),
                    ("sort_dir", "KEYWORD_ONLY", "str | None", required),
                ),
                "dict[str, Any]",
            ),
            "get_profile_hashtags": (
                (
                    ("platform", "KEYWORD_ONLY", "str", required),
                    ("account_handle", "KEYWORD_ONLY", "str", required),
                    (
                        "window",
                        "KEYWORD_ONLY",
                        "Literal['all', '30d', '365d'] | None",
                        required,
                    ),
                    (
                        "assignment_status",
                        "KEYWORD_ONLY",
                        "Literal['all', 'assigned', 'unassigned'] | None",
                        required,
                    ),
                ),
                "dict[str, Any]",
            ),
            "get_profile_hashtag_timeline": (
                (
                    ("platform", "KEYWORD_ONLY", "str", required),
                    ("account_handle", "KEYWORD_ONLY", "str", required),
                    (
                        "window",
                        "KEYWORD_ONLY",
                        "Literal['all', '30d', '365d'] | None",
                        required,
                    ),
                ),
                "dict[str, Any]",
            ),
            "get_profile_collaborators_tags": (
                (
                    ("platform", "KEYWORD_ONLY", "str", required),
                    ("account_handle", "KEYWORD_ONLY", "str", required),
                ),
                "dict[str, Any]",
            ),
            "get_catalog_posts": (
                (
                    ("platform", "KEYWORD_ONLY", "str", required),
                    ("account_handle", "KEYWORD_ONLY", "str", required),
                    ("page", "KEYWORD_ONLY", "int", required),
                    ("page_size", "KEYWORD_ONLY", "int", required),
                    (
                        "assignment_status",
                        "KEYWORD_ONLY",
                        (
                            "Literal['assigned', 'unassigned', "
                            "'ambiguous', 'needs_review'] | None"
                        ),
                        required,
                    ),
                ),
                "dict[str, Any]",
            ),
            "get_catalog_post_detail": (
                (
                    ("platform", "KEYWORD_ONLY", "str", required),
                    ("account_handle", "KEYWORD_ONLY", "str", required),
                    ("source_id", "KEYWORD_ONLY", "str", required),
                ),
                "dict[str, Any]",
            ),
            "get_catalog_review_queue": (
                (
                    ("platform", "KEYWORD_ONLY", "str", required),
                    ("account_handle", "KEYWORD_ONLY", "str", required),
                ),
                "dict[str, Any]",
            ),
            "get_catalog_run_progress": (
                (
                    ("platform", "KEYWORD_ONLY", "str", required),
                    ("account_handle", "KEYWORD_ONLY", "str", required),
                    ("run_id", "KEYWORD_ONLY", "str", required),
                    ("recent_log_limit", "KEYWORD_ONLY", "int", required),
                    ("fast", "KEYWORD_ONLY", "bool", required),
                ),
                "dict[str, Any]",
            ),
            "get_catalog_verification": (
                (
                    ("platform", "KEYWORD_ONLY", "str", required),
                    ("account_handle", "KEYWORD_ONLY", "str", required),
                    ("run_id", "KEYWORD_ONLY", "str | None", required),
                ),
                "dict[str, Any]",
            ),
            "get_catalog_gap_analysis_status": (
                (
                    ("platform", "KEYWORD_ONLY", "str", required),
                    ("account_handle", "KEYWORD_ONLY", "str", required),
                ),
                "dict[str, Any]",
            ),
            "get_catalog_freshness": (
                (
                    ("platform", "KEYWORD_ONLY", "str", required),
                    ("account_handle", "KEYWORD_ONLY", "str", required),
                    (
                        "use_cached_live_total_only",
                        "KEYWORD_ONLY",
                        "bool",
                        False,
                    ),
                    (
                        "statement_timeout_ms",
                        "KEYWORD_ONLY",
                        "int",
                        3000,
                    ),
                ),
                "dict[str, Any]",
            ),
        }}

        def signature_contract(function):
            signature = inspect.signature(function)
            parameters = tuple(
                (
                    parameter.name,
                    parameter.kind.name,
                    parameter.annotation,
                    (
                        required
                        if parameter.default is inspect.Parameter.empty
                        else parameter.default
                    ),
                )
                for parameter in signature.parameters.values()
            )
            return parameters, signature.return_annotation

        assert len(signature_contracts) == 15
        for wrapper_name, expected_signature in signature_contracts.items():
            assert (
                signature_contract(getattr(profile_reads, wrapper_name))
                == expected_signature
            )

        common = {{"platform": "instagram", "account_handle": "x"}}
        cases = [
            (
                "get_profile_summary",
                "get_social_account_profile_summary",
                {{**common, "detail": "full"}},
            ),
            (
                "get_live_profile_total",
                "get_social_account_live_profile_total",
                common,
            ),
            (
                "get_profile_posts",
                "get_social_account_profile_posts",
                {{
                    **common,
                    "page": 1,
                    "page_size": 2,
                    "search": None,
                    "comments_only": False,
                    "comment_filter": None,
                    "sort_by": None,
                    "sort_dir": None,
                }},
            ),
            (
                "get_profile_comments",
                "get_social_account_profile_comments",
                {{
                    **common,
                    "page": 1,
                    "page_size": 2,
                    "post_source_id": None,
                    "search": None,
                    "sort_by": None,
                    "sort_dir": None,
                }},
            ),
            (
                "get_profile_hashtags",
                "get_social_account_profile_hashtags",
                {{**common, "window": None, "assignment_status": None}},
            ),
            (
                "get_profile_hashtag_timeline",
                "get_social_account_profile_hashtag_timeline",
                {{**common, "window": None}},
            ),
            (
                "get_profile_collaborators_tags",
                "get_social_account_profile_collaborators_tags",
                common,
            ),
            (
                "get_catalog_posts",
                "get_social_account_catalog_posts",
                {{
                    **common,
                    "page": 1,
                    "page_size": 2,
                    "assignment_status": None,
                }},
            ),
            (
                "get_catalog_post_detail",
                "get_social_account_catalog_post_detail",
                {{**common, "source_id": "p"}},
            ),
            (
                "get_catalog_review_queue",
                "get_social_account_catalog_review_queue",
                common,
            ),
            (
                "get_catalog_run_progress",
                "get_social_account_catalog_run_progress",
                {{
                    **common,
                    "run_id": "r",
                    "recent_log_limit": 5,
                    "fast": True,
                }},
            ),
            (
                "get_catalog_verification",
                "get_social_account_catalog_verification",
                {{**common, "run_id": None}},
            ),
            (
                "get_catalog_gap_analysis_status",
                "get_social_account_catalog_gap_analysis_status",
                common,
            ),
            (
                "get_catalog_freshness",
                "get_social_account_catalog_freshness",
                {{
                    **common,
                    "use_cached_live_total_only": False,
                    "statement_timeout_ms": 3000,
                }},
            ),
        ]
        for wrapper_name, implementation_name, kwargs in cases:
            marker = object()
            calls = []

            def replacement(
                *args,
                _marker=marker,
                _calls=calls,
                **received,
            ):
                _calls.append((args, received))
                return _marker

            setattr(repository, implementation_name, replacement)
            assert getattr(profile_reads, wrapper_name)(**kwargs) is marker
            assert calls == [((), kwargs)]

            expected = RuntimeError(
                f"{{wrapper_name}} exact exception identity"
            )

            def fail(
                *args,
                _expected=expected,
                **received,
            ):
                assert args == ()
                assert received == kwargs
                raise _expected

            setattr(repository, implementation_name, fail)
            try:
                getattr(profile_reads, wrapper_name)(**kwargs)
            except RuntimeError as error:
                assert error is expected
            else:
                raise AssertionError(
                    f"expected exact exception passthrough for {{wrapper_name}}"
                )

        late_marker = object()
        repository._normalize_social_account_profile_summary_detail = (
            lambda value: late_marker
        )
        assert profile_reads.normalize_profile_summary_detail(None) is late_marker

        expected = RuntimeError(
            "normalize_profile_summary_detail exact exception identity"
        )

        def fail(value):
            raise expected

        repository._normalize_social_account_profile_summary_detail = fail
        try:
            profile_reads.normalize_profile_summary_detail(None)
        except RuntimeError as error:
            assert error is expected
        else:
            raise AssertionError(
                "expected exact exception passthrough for "
                "normalize_profile_summary_detail"
            )
        """
    )


def test_account_profile_page_size_definition_publication_and_rollback() -> None:
    _run_fresh_python(
        """
        import importlib
        import inspect

        common = importlib.import_module(
            "trr_backend.socials.read_models.account_profile.common"
        )
        provider_name = "trr_backend.socials.social_season_analytics_impl"
        assert common._PROVIDER_STATE == "UNCONFIGURED"
        assert provider_name not in __import__("sys").modules
        posts_default = inspect.signature(
            common.get_social_account_profile_posts
        ).parameters["page_size"].default
        comments_default = inspect.signature(
            common.get_social_account_profile_comments
        ).parameters["page_size"].default
        assert type(posts_default) is int and posts_default == 25
        assert type(comments_default) is int and comments_default == 25
        local_binding = common._SOCIAL_ACCOUNT_PROFILE_DEFAULT_PAGE_SIZE
        assert type(local_binding) is int and local_binding == 25

        provider = {
            name: (lambda *args, **kwargs: None)
            for name in common._LOCAL_ROOM_NAMES
        }
        provider["_SOCIAL_ACCOUNT_PROFILE_DEFAULT_PAGE_SIZE"] = int("25")
        original_publish = common._publish_provider_binding

        class StopPublication(BaseException):
            pass

        def stop_after_page_size(name, value):
            original_publish(name, value)
            if name == "_SOCIAL_ACCOUNT_PROFILE_DEFAULT_PAGE_SIZE":
                raise StopPublication("page-size publication stop")

        common._publish_provider_binding = stop_after_page_size
        try:
            common._configure_legacy_provider(provider)
        except StopPublication as error:
            assert str(error) == "page-size publication stop"
        else:
            raise AssertionError("page-size publication did not stop")
        finally:
            common._publish_provider_binding = original_publish

        assert common._PROVIDER_STATE == "UNCONFIGURED"
        assert common._PROVIDER_NAMESPACE is None
        assert common._SOCIAL_ACCOUNT_PROFILE_DEFAULT_PAGE_SIZE is local_binding
        assert type(common._SOCIAL_ACCOUNT_PROFILE_DEFAULT_PAGE_SIZE) is int
        assert common._SOCIAL_ACCOUNT_PROFILE_DEFAULT_PAGE_SIZE == 25

        common._configure_legacy_provider(provider)
        assert common._PROVIDER_STATE == "READY"
        assert common._PROVIDER_NAMESPACE is provider
        assert (
            common._SOCIAL_ACCOUNT_PROFILE_DEFAULT_PAGE_SIZE
            is provider["_SOCIAL_ACCOUNT_PROFILE_DEFAULT_PAGE_SIZE"]
        )
        assert type(posts_default) is int and posts_default == 25
        assert type(comments_default) is int and comments_default == 25
        """
    )


def test_profile_dashboard_defers_progress_and_preserves_analytics_repo_patch_seam() -> None:
    _run_fresh_python(
        """
        import importlib
        import sys

        dashboard_name = "trr_backend.socials.profile_dashboard"
        common_name = "trr_backend.socials.read_models.account_profile.common"
        provider_name = "trr_backend.socials.social_season_analytics_impl"
        progress_name = "trr_backend.socials.pipelines.account_catalog.progress"
        dashboard = importlib.import_module(dashboard_name)
        common = sys.modules[common_name]
        assert common._PROVIDER_STATE == "UNCONFIGURED"
        assert provider_name not in sys.modules
        assert progress_name not in sys.modules
        try:
            dashboard.analytics_repo.get_social_account_catalog_run_progress(
                "instagram", "bravotv", "run-1"
            )
        except RuntimeError as error:
            assert str(error) == (
                "ACCOUNT_PROFILE_PROVIDER_UNCONFIGURED: "
                "trr_backend.socials.social_season_analytics_impl has not finished loading"
            )
        else:
            raise AssertionError("pre-READY progress call did not fail")
        assert provider_name not in sys.modules
        assert progress_name not in sys.modules

        importlib.import_module(provider_name)
        progress = importlib.import_module(progress_name)
        progress_calls = []

        def progress_replacement(*args, **kwargs):
            progress_calls.append((args, kwargs))
            return {"run_id": "run-1"}

        original_progress = progress.get_social_account_catalog_run_progress
        progress.get_social_account_catalog_run_progress = progress_replacement
        try:
            assert dashboard.analytics_repo.get_social_account_catalog_run_progress(
                "instagram", "bravotv", "run-1", fast=True
            ) == {"run_id": "run-1"}
        finally:
            progress.get_social_account_catalog_run_progress = original_progress
        assert progress_calls == [
            (("instagram", "bravotv", "run-1"), {"fast": True})
        ]

        summary_calls = []
        dashboard.analytics_repo.get_social_account_profile_summary = (
            lambda **kwargs: summary_calls.append(kwargs)
            or {"catalog_recent_runs": [{"run_id": "run-2", "status": "running"}]}
        )
        dashboard.analytics_repo.get_social_account_catalog_run_progress = (
            lambda *args, **kwargs: {"args": args, "kwargs": kwargs}
        )
        payload = dashboard.build_social_account_profile_dashboard(
            platform="instagram",
            account_handle="bravotv",
            detail="full",
            run_id=None,
            recent_log_limit=7,
        )
        assert summary_calls == [
            {
                "platform": "instagram",
                "account_handle": "bravotv",
                "detail": "full",
            }
        ]
        assert payload["data"]["catalog_run_progress"] == {
            "args": ("instagram", "bravotv", "run-2"),
            "kwargs": {"recent_log_limit": 7, "fast": True},
        }
        """
    )


def test_account_profile_operator_exports_reject_unconfigured_before_side_effects() -> None:
    _run_fresh_python(
        """
        import importlib
        import sys
        from types import SimpleNamespace

        common_name = "trr_backend.socials.read_models.account_profile.common"
        provider_name = "trr_backend.socials.social_season_analytics_impl"
        common = importlib.import_module(common_name)
        side_effects = []

        def forbidden(*args, **kwargs):
            side_effects.append((args, kwargs))
            raise AssertionError("database side effect reached")

        common.pg = SimpleNamespace(
            db_connection=forbidden,
            db_read_connection=forbidden,
        )
        calls = (
            lambda: common.instagram_comment_rollup_health(sample_limit=7),
            lambda: common.rebuild_instagram_post_comment_rollups(
                account_handle="BravoTV",
                post_ids=["post-1"],
                limit=3,
                dry_run=True,
            ),
        )
        for call in calls:
            try:
                call()
            except RuntimeError as error:
                assert str(error) == (
                    "ACCOUNT_PROFILE_PROVIDER_UNCONFIGURED: "
                    "trr_backend.socials.social_season_analytics_impl has not finished loading"
                )
            else:
                raise AssertionError("pre-READY operator export did not fail")
        assert side_effects == []
        assert provider_name not in sys.modules
        assert common._PROVIDER_STATE == "UNCONFIGURED"
        """
    )
