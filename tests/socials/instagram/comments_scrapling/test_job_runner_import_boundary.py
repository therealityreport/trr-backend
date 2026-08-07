from __future__ import annotations

import ast
import os
import subprocess
import sys
import textwrap
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[4]
RUNNER_PATH = BACKEND_ROOT / "trr_backend" / "socials" / "instagram" / "comments_scrapling" / "job_runner.py"
RUNNER_MODULE = "trr_backend.socials.instagram.comments_scrapling.job_runner"
DISPATCH_MODULE = "trr_backend.socials.control_plane.dispatch_runtime"
REPOSITORY_ALIAS_MODULE = "trr_backend.repositories.social_season_analytics"
LEGACY_IMPL_MODULE = "trr_backend.socials.social_season_analytics_impl"
REPO_ATTRIBUTES = (
    "INSTAGRAM_COMMENTS_SCRAPLING_STAGE",
    "_instagram_filter_incomplete_comment_targets",
    "_instagram_reported_comments_sql",
    "_merge_catalog_run_config",
    "_reconcile_post_comment_count",
    "_update_job_config",
)


def _run_fresh_process(script: str) -> None:
    env = os.environ.copy()
    existing_pythonpath = env.get("PYTHONPATH")
    env["PYTHONPATH"] = os.pathsep.join(path for path in (str(BACKEND_ROOT), existing_pythonpath) if path)
    try:
        subprocess.run(
            [sys.executable, "-c", textwrap.dedent(script)],
            cwd=BACKEND_ROOT,
            env=env,
            check=True,
            text=True,
            capture_output=True,
        )
    except subprocess.CalledProcessError as exc:
        stdout = (exc.stdout or "")[-4_000:]
        stderr = (exc.stderr or "")[-4_000:]
        raise AssertionError(
            f"fresh child process failed\nstdout:\n{stdout or '<empty>'}\nstderr:\n{stderr or '<empty>'}"
        ) from exc


def test_runner_source_keeps_the_exact_function_scoped_import_signature_and_repo_alias() -> None:
    source = RUNNER_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    functions = {node.name: node for node in tree.body if isinstance(node, ast.FunctionDef)}
    runner = functions["run_instagram_comments_scrapling_job"]

    boundary_import = runner.body[0]
    assert isinstance(boundary_import, ast.ImportFrom)
    assert boundary_import.level == 0
    assert boundary_import.module == "trr_backend.socials.control_plane.dispatch_runtime"
    assert [(alias.name, alias.asname) for alias in boundary_import.names] == [("legacy", "repo")]

    module_scoped_imports = [node for node in tree.body if isinstance(node, (ast.Import, ast.ImportFrom))]
    assert all(
        not (isinstance(node, ast.ImportFrom) and node.module == "trr_backend.socials.control_plane.dispatch_runtime")
        for node in module_scoped_imports
    )
    imported_targets = {
        imported.name
        if isinstance(node, ast.Import)
        else f"{node.module}.{imported.name}"
        if node.module
        else imported.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.Import, ast.ImportFrom))
        for imported in node.names
    }
    assert REPOSITORY_ALIAS_MODULE not in imported_targets
    assert LEGACY_IMPL_MODULE not in imported_targets

    arguments = runner.args
    assert arguments.posonlyargs == []
    assert [argument.arg for argument in arguments.args] == ["job"]
    assert arguments.args[0].annotation is not None
    assert ast.unparse(arguments.args[0].annotation) == "dict[str, Any]"
    assert arguments.defaults == []
    assert arguments.vararg is None
    assert [argument.arg for argument in arguments.kwonlyargs] == ["worker_id"]
    assert arguments.kwonlyargs[0].annotation is not None
    assert ast.unparse(arguments.kwonlyargs[0].annotation) == "str | None"
    assert len(arguments.kw_defaults) == 1
    assert isinstance(arguments.kw_defaults[0], ast.Constant)
    assert arguments.kw_defaults[0].value is None
    assert arguments.kwarg is None
    assert runner.returns is not None
    assert ast.unparse(runner.returns) == "dict[str, Any]"

    repo_attributes = {
        node.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id == "repo"
    }
    assert repo_attributes == set(REPO_ATTRIBUTES)

    direct_runner_attributes = {
        node.attr
        for node in ast.walk(runner)
        if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id == "repo"
    }
    assert direct_runner_attributes == {
        "INSTAGRAM_COMMENTS_SCRAPLING_STAGE",
        "_instagram_filter_incomplete_comment_targets",
        "_reconcile_post_comment_count",
        "_update_job_config",
    }

    helper_attributes = {
        "_load_expected_comment_counts": "_instagram_reported_comments_sql",
        "_recommend_public_blocked_pause": "_merge_catalog_run_config",
        "_filter_retryable_incomplete_targets_against_current_db": ("_instagram_filter_incomplete_comment_targets"),
    }
    for helper_name, attribute_name in helper_attributes.items():
        helper = functions[helper_name]
        assert any(
            isinstance(node, ast.Attribute)
            and isinstance(node.value, ast.Name)
            and node.value.id == "repo"
            and node.attr == attribute_name
            for node in ast.walk(helper)
        )
        assert any(
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == helper_name
            and any(
                keyword.arg == "repo" and isinstance(keyword.value, ast.Name) and keyword.value.id == "repo"
                for keyword in node.keywords
            )
            for node in ast.walk(runner)
        )


def test_cold_import_and_first_call_load_the_exact_module_proxy_without_partial_state() -> None:
    _run_fresh_process(
        f"""
        import importlib
        import sys

        runner_name = {RUNNER_MODULE!r}
        dispatch_name = {DISPATCH_MODULE!r}
        alias_name = {REPOSITORY_ALIAS_MODULE!r}
        impl_name = {LEGACY_IMPL_MODULE!r}
        deferred_names = (dispatch_name, alias_name, impl_name)

        assert all(name not in sys.modules for name in deferred_names)
        runner = importlib.import_module(runner_name)
        assert all(name not in sys.modules for name in deferred_names)

        job = {{"id": "job-boundary", "run_id": "run-boundary", "config": {{"account": ""}}}}
        try:
            runner.run_instagram_comments_scrapling_job(job)
        except runner.CommentsScraplingRuntimeError as exc:
            assert str(exc) == "Instagram comments Scrapling job is missing an account handle."
            assert exc.error_code == "instagram_comments_account_missing"
            assert exc.retryable is False
        else:
            raise AssertionError("missing-account branch did not raise CommentsScraplingRuntimeError")

        alias = importlib.import_module(alias_name)
        dispatch = sys.modules[dispatch_name]
        impl = sys.modules[impl_name]
        assert dispatch.legacy is alias is impl
        assert dispatch.legacy.__dict__ is alias.__dict__ is impl.__dict__
        assert all(
            not getattr(getattr(module, "__spec__", None), "_initializing", False)
            for module in (dispatch, alias, impl)
        )
        """
    )


def test_early_late_and_between_call_monkeypatches_share_all_six_proxy_attributes() -> None:
    _run_fresh_process(
        f"""
        import importlib
        import sys

        runner_name = {RUNNER_MODULE!r}
        dispatch_name = {DISPATCH_MODULE!r}
        alias_name = {REPOSITORY_ALIAS_MODULE!r}
        impl_name = {LEGACY_IMPL_MODULE!r}
        repo_attributes = {REPO_ATTRIBUTES!r}

        def patch_all(module, prefix):
            for name in repo_attributes:
                setattr(module, name, f"{{prefix}}:{{name}}")

        def assert_all(module, prefix):
            for name in repo_attributes:
                assert getattr(module, name) == f"{{prefix}}:{{name}}"

        alias = importlib.import_module(alias_name)
        patch_all(alias, "before-runner-import")
        assert dispatch_name in sys.modules
        assert sys.modules[dispatch_name].legacy is alias

        runner = importlib.import_module(runner_name)
        dispatch = sys.modules[dispatch_name]
        impl = sys.modules[impl_name]
        assert dispatch.legacy is alias is impl
        assert_all(dispatch.legacy, "before-runner-import")

        patch_all(dispatch.legacy, "after-runner-import")
        assert_all(alias, "after-runner-import")
        job = {{"id": "job-boundary", "run_id": "run-boundary", "config": {{"account": ""}}}}

        def invoke_missing_account():
            try:
                runner.run_instagram_comments_scrapling_job(job)
            except runner.CommentsScraplingRuntimeError as exc:
                assert exc.error_code == "instagram_comments_account_missing"
            else:
                raise AssertionError("missing-account branch did not raise")

        invoke_missing_account()
        assert_all(alias, "after-runner-import")

        patch_all(alias, "after-first-call")
        assert_all(dispatch.legacy, "after-first-call")
        invoke_missing_account()
        assert_all(dispatch.legacy, "after-first-call")

        patch_all(dispatch.legacy, "between-calls")
        assert_all(alias, "between-calls")
        invoke_missing_account()
        assert_all(alias, "between-calls")
        assert dispatch.legacy.__dict__ is alias.__dict__ is impl.__dict__
        """
    )
